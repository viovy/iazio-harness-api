package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/viovy/iazio-harness-api/internal/codebase"
)

// CodebaseSearchHandler handles POST /api/v1/codebase/search.
type CodebaseSearchHandler struct {
	Store codebase.Store
}

// NewCodebaseSearchHandler creates a new CodebaseSearchHandler.
func NewCodebaseSearchHandler(store codebase.Store) *CodebaseSearchHandler {
	return &CodebaseSearchHandler{Store: store}
}

// SearchPayload represents the incoming JSON request for codebase search.
type SearchPayload struct {
	Query      string `json:"query"`
	RepoName   string `json:"repo_name,omitempty"`
	PathGlob   string `json:"path_glob,omitempty"`
	GitTreeSHA string `json:"git_tree_sha,omitempty"`
	Limit      int    `json:"limit,omitempty"`
	Cursor     string `json:"cursor,omitempty"`
}

func (h *CodebaseSearchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read request body"})
		return
	}
	defer r.Body.Close()

	var payload SearchPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "invalid json payload: " + err.Error()})
		return
	}

	trimmedQuery := strings.TrimSpace(payload.Query)
	if trimmedQuery == "" {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "query must not be empty"})
		return
	}

	limit := payload.Limit
	if limit <= 0 {
		limit = 10
	} else if limit > 50 {
		limit = 50
	}

	req := codebase.SearchRequest{
		Query:      trimmedQuery,
		RepoName:   strings.TrimSpace(payload.RepoName),
		PathGlob:   strings.TrimSpace(payload.PathGlob),
		GitTreeSHA: strings.TrimSpace(payload.GitTreeSHA),
		Limit:      limit,
		Cursor:     strings.TrimSpace(payload.Cursor),
	}

	result, err := h.Store.SearchChunks(r.Context(), req)
	if err != nil {
		if strings.Contains(err.Error(), "query") {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}

	writeJSON(w, http.StatusOK, result)
}
