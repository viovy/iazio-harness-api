package control

import (
	"testing"
	"time"
)

func TestGetRehydrationBundle(t *testing.T) {
	e := NewEngine(func() time.Time { return time.Date(2026, 10, 25, 6, 0, 0, 0, time.UTC) })

	// Empty conversation ID should fail
	_, err := e.GetRehydrationBundle("")
	if err == nil {
		t.Fatalf("expected error for empty conversation id")
	}

	// Unknown conversation returns minimal bundle
	b1, err := e.GetRehydrationBundle("conv-unknown-123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b1.ConversationID != "conv-unknown-123" {
		t.Errorf("expected conversation_id conv-unknown-123, got: %s", b1.ConversationID)
	}
	if b1.BootstrapPrompt == "" {
		t.Errorf("expected non-empty bootstrap prompt")
	}

	// Register a repo and lease a job
	_ = e.RegisterHost("host-1", "Host 1")
	_ = e.UpsertRepo(Repo{
		HostID:       "host-1",
		WorktreePath: "/repos/app-01",
	})
	p := e.PutPrompt(Prompt{
		Title:  "Feature X",
		Body:   "Implement feature X\n\nAcceptance Criteria:\n- [ ] Gate 1 passes\n- [ ] Gate 2 passes\n",
		Status: "READY",
	})
	sch, err := e.ExecutePrompt(p.ID, "host-1", "/repos/app-01", 1, nil)
	if err != nil {
		t.Fatalf("execute prompt failed: %v", err)
	}
	job, _, _, ok := e.PollHost("host-1")
	if !ok {
		t.Fatalf("poll host failed to lease job")
	}

	convID := "conv-active-aad902c8"
	_ = e.AddJobConversationWithOptions(job.ID, convID, ConversationAttachOptions{
		Role:     "initial_resumed",
		ASEPhase: "IMPLEMENTATION",
	})

	// Set InterruptedASEState and WorkspaceStateManifest on job
	e.mu.Lock()
	if j, ok := e.jobs[job.ID]; ok {
		j.InterruptedASEState = map[string]any{
			"phase":            "IMPLEMENTATION",
			"step":             3,
			"completed_phases": []string{"PHASE_0_BIND", "PHASE_1_DISCOVERY"},
			"unmet_gates":      []string{"Gate 1 passes", "Gate 2 passes"},
		}
		j.WorkspaceStateManifest = map[string]any{
			"branch":                "chore/gov-3686",
			"head_sha":              "abcdef123456",
			"preserved_files_count": 2,
			"modified_files":        []string{"file1.go", "file2.go"},
		}
	}
	e.mu.Unlock()

	// Append log chunks with tool actions
	e.AppendLog(job.ID, LogChunk{Type: "OUTPUT_CHUNK", Text: "Running git status in /repos/app-01"})
	e.AppendLog(job.ID, LogChunk{Type: "OUTPUT_CHUNK", Text: "Executing go test ./... -> PASS"})
	e.AppendLog(job.ID, LogChunk{Type: "OUTPUT_CHUNK", Text: "File edited: file1.go with 12 lines"})

	bundle, err := e.GetRehydrationBundle(convID)
	if err != nil {
		t.Fatalf("GetRehydrationBundle failed: %v", err)
	}

	if bundle.JobID != job.ID {
		t.Errorf("expected job ID %s, got %s", job.ID, bundle.JobID)
	}
	if bundle.InterruptedPhase != "IMPLEMENTATION" {
		t.Errorf("expected interrupted phase IMPLEMENTATION, got %s", bundle.InterruptedPhase)
	}
	if len(bundle.CompletedPhases) != 2 {
		t.Errorf("expected 2 completed phases, got: %v", bundle.CompletedPhases)
	}
	if len(bundle.RemainingChecklist) != 2 {
		t.Errorf("expected 2 remaining checklist items, got: %v", bundle.RemainingChecklist)
	}
	if len(bundle.LastToolActions) < 3 {
		t.Errorf("expected at least 3 tool actions, got %d", len(bundle.LastToolActions))
	}
	if bundle.WorkspaceStateManifest == nil || bundle.WorkspaceStateManifest["branch"] != "chore/gov-3686" {
		t.Errorf("expected workspace manifest branch chore/gov-3686, got: %v", bundle.WorkspaceStateManifest)
	}
	if bundle.BootstrapPrompt == "" {
		t.Errorf("expected non-empty bootstrap prompt")
	}
	_ = sch
}
