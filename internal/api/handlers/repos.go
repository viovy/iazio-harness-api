// Package handlers provides HTTP handler endpoints for fleet discovery and codebase search.
package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/viovy/iazio-harness-api/internal/codebase"
)

// ReposHandler handles GET /api/v1/repos.
type ReposHandler struct {
	Store codebase.Store
}

// NewReposHandler creates a new ReposHandler.
func NewReposHandler(store codebase.Store) *ReposHandler {
	return &ReposHandler{Store: store}
}

func (h *ReposHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	query := r.URL.Query().Get("query")
	cursor := r.URL.Query().Get("cursor")
	limitStr := r.URL.Query().Get("limit")

	limit := 20
	if limitStr != "" {
		parsed, err := strconv.Atoi(limitStr)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid limit parameter"})
			return
		}
		limit = parsed
	}

	req := codebase.ListReposRequest{
		Query:  query,
		Cursor: cursor,
		Limit:  limit,
	}

	result, err := h.Store.ListRepositories(r.Context(), req)
	if err != nil {
		if strings.Contains(err.Error(), "cursor") {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
