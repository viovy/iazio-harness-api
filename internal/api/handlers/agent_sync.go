// Package handlers provides HTTP handler endpoints for fleet discovery, codebase search, and agent sync.
package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/viovy/iazio-harness-api/internal/codebase"
)

// RepoManifest describes a submodule manifest reported by an agent.
type RepoManifest struct {
	Name          string `json:"name"`
	Path          string `json:"path"`
	DefaultBranch string `json:"default_branch"`
	GitTreeSHA    string `json:"git_tree_sha"`
	Description   string `json:"description,omitempty"`
}

// RepoSyncRequest represents the Phase 1 agent repository handshake payload.
type RepoSyncRequest struct {
	HostID string         `json:"host_id"`
	Repos  []RepoManifest `json:"repos"`
}

// OutOfSyncRepo identifies a repository requiring delta chunk synchronization.
type OutOfSyncRepo struct {
	RepoName       string `json:"repo_name"`
	IndexedTreeSHA string `json:"indexed_tree_sha"`
	TargetTreeSHA  string `json:"target_tree_sha"`
}

// RepoSyncResponse contains the subset of repositories requiring chunk export.
type RepoSyncResponse struct {
	OutOfSync []OutOfSyncRepo `json:"out_of_sync"`
}

// AgentSyncHandler manages agent handshake and codebase chunk ingestion endpoints.
type AgentSyncHandler struct {
	Store    codebase.Store
	Ingestor codebase.Ingestor
}

// NewAgentSyncHandler constructs a new AgentSyncHandler.
func NewAgentSyncHandler(store codebase.Store, ingestor codebase.Ingestor) *AgentSyncHandler {
	if ingestor == nil {
		ingestor = codebase.NewIngestor(store, nil)
	}
	return &AgentSyncHandler{
		Store:    store,
		Ingestor: ingestor,
	}
}

// SyncRepos handles POST /api/v1/agent/repos/sync.
// It registers/upserts fleet repository metadata and returns repositories whose stored
// indexed_tree_sha differs from the agent's reported git_tree_sha.
func (h *AgentSyncHandler) SyncRepos(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	var req RepoSyncRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	var outOfSync []OutOfSyncRepo
	for _, repo := range req.Repos {
		name := strings.TrimSpace(repo.Name)
		if name == "" {
			continue
		}

		existing, found, err := h.Store.GetRepository(r.Context(), name)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}

		indexedTreeSHA := ""
		if found && existing != nil {
			indexedTreeSHA = existing.GitTreeSHA
			// Preserve existing GitTreeSHA (indexed_tree_sha), update metadata
			existing.Path = repo.Path
			if repo.DefaultBranch != "" {
				existing.DefaultBranch = repo.DefaultBranch
			}
			if repo.Description != "" {
				existing.Description = repo.Description
			}
			if req.HostID != "" {
				existing.HostID = req.HostID
			}
			existing.UpdatedAt = time.Now().UTC()
			if err := h.Store.UpsertRepository(r.Context(), *existing); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
		} else {
			// Initial registration: store with empty GitTreeSHA (indexed_tree_sha)
			branch := repo.DefaultBranch
			if branch == "" {
				branch = "main"
			}
			newRepo := codebase.Repository{
				Name:          name,
				Path:          repo.Path,
				DefaultBranch: branch,
				GitTreeSHA:    "",
				Description:   repo.Description,
				HostID:        req.HostID,
				UpdatedAt:     time.Now().UTC(),
			}
			if err := h.Store.UpsertRepository(r.Context(), newRepo); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
		}

		// Check if out of sync
		if indexedTreeSHA == "" || indexedTreeSHA != repo.GitTreeSHA {
			outOfSync = append(outOfSync, OutOfSyncRepo{
				RepoName:       name,
				IndexedTreeSHA: indexedTreeSHA,
				TargetTreeSHA:  repo.GitTreeSHA,
			})
		}
	}

	if outOfSync == nil {
		outOfSync = []OutOfSyncRepo{}
	}

	writeJSON(w, http.StatusOK, RepoSyncResponse{OutOfSync: outOfSync})
}

// SyncCodebase handles POST /api/v1/agent/codebase/sync.
// It ingests a batch of deleted file paths and added/modified code chunks for a repository,
// performing content-addressed embedding deduplication and promoting indexed_tree_sha on final batch.
func (h *AgentSyncHandler) SyncCodebase(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	var req codebase.IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	if strings.TrimSpace(req.RepoName) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "repo_name is required"})
		return
	}

	res, err := h.Ingestor.Ingest(r.Context(), req)
	if err != nil {
		var mismatch *codebase.ErrBaseTreeMismatch
		if errors.As(err, &mismatch) {
			writeJSON(w, http.StatusConflict, map[string]any{
				"error":            "base_tree_sha mismatch",
				"repo_name":        mismatch.RepoName,
				"base_tree_sha":    mismatch.BaseTreeSHA,
				"indexed_tree_sha": mismatch.IndexedTreeSHA,
			})
			return
		}

		var notFound *codebase.ErrRepoNotFound
		if errors.As(err, &notFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": notFound.Error()})
			return
		}

		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, res)
}
