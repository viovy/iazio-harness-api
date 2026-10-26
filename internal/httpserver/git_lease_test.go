package httpserver

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
	"github.com/viovy/iazio-harness-api/internal/gitlease"
)

func TestGitLeaseEndpoints_AcquireRenewRelease(t *testing.T) {
	mgr := gitlease.NewManager()
	srv := &Server{
		Engine:          control.NewEngine(nil),
		Queue:           dispatch.NewInMemoryQueue(),
		GitLeaseManager: mgr,
	}
	handler := srv.Handler()

	// 1. Acquire lease for viovy/meta-repo
	acquireBody := []byte(`{"repo":"viovy/meta-repo","holder":"mac-mini/meta-repo-03","ttl_seconds":60}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/git-leases/acquire", bytes.NewReader(acquireBody))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}
	var acqResp struct {
		Acquired bool            `json:"acquired"`
		Lease    *gitlease.Lease `json:"lease"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &acqResp); err != nil {
		t.Fatalf("failed unmarshaling acquire response: %v", err)
	}
	if !acqResp.Acquired || acqResp.Lease == nil || acqResp.Lease.LeaseID == "" {
		t.Fatalf("expected acquired lease with lease_id, got %+v", acqResp)
	}
	leaseID := acqResp.Lease.LeaseID

	// 2. Conflicting acquire from another holder must return 409 Conflict
	conflictBody := []byte(`{"repo":"viovy/meta-repo","holder":"pc1/meta-repo-03","ttl_seconds":60}`)
	reqConf := httptest.NewRequest(http.MethodPost, "/api/v1/git-leases/acquire", bytes.NewReader(conflictBody))
	recConf := httptest.NewRecorder()
	handler.ServeHTTP(recConf, reqConf)

	if recConf.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict on concurrent acquire, got %d: %s", recConf.Code, recConf.Body.String())
	}

	// 3. Renew lease
	renewBody, _ := json.Marshal(map[string]any{
		"repo":        "viovy/meta-repo",
		"holder":      "mac-mini/meta-repo-03",
		"lease_id":    leaseID,
		"ttl_seconds": 90,
	})
	reqRenew := httptest.NewRequest(http.MethodPost, "/api/v1/git-leases/renew", bytes.NewReader(renewBody))
	recRenew := httptest.NewRecorder()
	handler.ServeHTTP(recRenew, reqRenew)

	if recRenew.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on renew, got %d: %s", recRenew.Code, recRenew.Body.String())
	}

	// 4. List leases
	reqList := httptest.NewRequest(http.MethodGet, "/api/v1/git-leases", nil)
	recList := httptest.NewRecorder()
	handler.ServeHTTP(recList, reqList)

	if recList.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on list, got %d: %s", recList.Code, recList.Body.String())
	}
	var listResp struct {
		Leases []*gitlease.Lease `json:"leases"`
	}
	_ = json.Unmarshal(recList.Body.Bytes(), &listResp)
	if len(listResp.Leases) != 1 || listResp.Leases[0].Repo != "viovy/meta-repo" {
		t.Fatalf("unexpected list response: %+v", listResp)
	}

	// 5. Release lease
	relBody, _ := json.Marshal(map[string]any{
		"repo":     "viovy/meta-repo",
		"holder":   "mac-mini/meta-repo-03",
		"lease_id": leaseID,
	})
	reqRel := httptest.NewRequest(http.MethodPost, "/api/v1/git-leases/release", bytes.NewReader(relBody))
	recRel := httptest.NewRecorder()
	handler.ServeHTTP(recRel, reqRel)

	if recRel.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on release, got %d: %s", recRel.Code, recRel.Body.String())
	}

	// 6. After release, pc1 can acquire
	reqConf2 := httptest.NewRequest(http.MethodPost, "/api/v1/git-leases/acquire", bytes.NewReader(conflictBody))
	recConf2 := httptest.NewRecorder()
	handler.ServeHTTP(recConf2, reqConf2)

	if recConf2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on pc1 acquire after release, got %d: %s", recConf2.Code, recConf2.Body.String())
	}
}

func TestJobStream_LastEventID_Resumption(t *testing.T) {
	eng := control.NewEngine(nil)

	jobID := "job-stream-test"
	for i := 1; i <= 5; i++ {
		eng.AppendLog(jobID, control.LogChunk{
			Seq:    i,
			Type:   "OUTPUT_CHUNK",
			Stream: "stdout",
			Text:   "chunk line\n",
		})
	}

	// Verify TailLogs with afterSeq = 3 returns exactly chunks 4 and 5
	chunks, _, _ := eng.TailLogs(jobID, 100, 3)
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks after seq 3, got %d", len(chunks))
	}
	if chunks[0].Seq != 4 || chunks[1].Seq != 5 {
		t.Fatalf("unexpected chunk seqs: %d, %d", chunks[0].Seq, chunks[1].Seq)
	}
}
