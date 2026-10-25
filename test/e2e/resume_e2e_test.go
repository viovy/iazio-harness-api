package e2e_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/httpserver"
)

func TestE2E_ResumeExecutionLineage_AndScheduleAssociation(t *testing.T) {
	eng := control.NewEngine(nil)
	srv := &httpserver.Server{Engine: eng}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1. Seed host and repo in engine
	eng.RegisterHost("mac-mini", "permanent")
	repoPath := "/Users/romeo/work/repo-03"
	_ = eng.UpsertRepo(control.Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        control.QueueOpen,
		Lock:         control.LockIdle,
	})

	initialConvID := "aad902c8-bda6-44c7-8a68-c468a1562399"

	// 2. Dispatch initial job-1311 via prompt execution
	p := eng.PutPrompt(control.Prompt{
		ID:     "prompt-ase-1",
		Title:  "Autonomous Story Execution",
		Engine: "agy",
		Status: "READY",
	})
	sch1, err := eng.ExecutePromptWithResume(p.ID, "mac-mini", repoPath, 1, nil, initialConvID)
	if err != nil {
		t.Fatalf("failed to execute prompt: %v", err)
	}

	job1, _, _, ok := eng.PollHost("mac-mini")
	if !ok {
		t.Fatal("expected job1 on poll")
	}

	// Post conversation for job1 via HTTP endpoint
	conv1Payload := []byte(`{"conversation_id":"` + initialConvID + `","role":"initial_active","cli_args":"--conversation=` + initialConvID + `"}`)
	resp, err := http.Post(ts.URL+"/v1/jobs/"+job1.ID+"/conversations", "application/json", bytes.NewReader(conv1Payload))
	if err != nil || resp.StatusCode >= 300 {
		t.Fatalf("failed to post conversation: %v, status: %v", err, resp.StatusCode)
	}

	// Exit 1 on job1 (simulating interruption) via HTTP endpoint
	exit1Payload := []byte(`{"exit_code":1}`)
	resp, err = http.Post(ts.URL+"/v1/jobs/"+job1.ID+"/exit", "application/json", bytes.NewReader(exit1Payload))
	if err != nil || resp.StatusCode >= 300 {
		t.Fatalf("failed to post exit 1: %v, status: %v", err, resp.StatusCode)
	}

	// 3. Resume the repo in place via HTTP endpoint POST /v1/repos/{host}/resume-in-place
	resumeReq := []byte(`{"path":"` + repoPath + `","engine":"agy"}`)
	resp, err = http.Post(ts.URL+"/v1/repos/mac-mini/resume-in-place", "application/json", bytes.NewReader(resumeReq))
	if err != nil || resp.StatusCode >= 300 {
		t.Fatalf("failed to resume repo in place: %v, status: %v", err, resp.StatusCode)
	}
	var resSch map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&resSch)

	job2, _, _, ok := eng.PollHost("mac-mini")
	if !ok {
		t.Fatal("expected job2 on poll after resume")
	}
	t.Logf("job2 at poll: ID=%s ResumeMode=%s Initial=%s Active=%s", job2.ID, job2.ResumeMode, job2.InitialConversationID, job2.ActiveConversationID)

	// Verify 4-tuple contract and lineage on job 2
	if job2.InitialConversationID != initialConvID {
		t.Fatalf("expected job2 InitialConversationID=%s, got %s", initialConvID, job2.InitialConversationID)
	}
	if job2.ParentJobID != job1.ID {
		t.Fatalf("expected job2 ParentJobID=%s, got %s", job1.ID, job2.ParentJobID)
	}
	if job2.ExecutionAttempt != 2 {
		t.Fatalf("expected job2 ExecutionAttempt=2, got %d", job2.ExecutionAttempt)
	}
	if len(job2.ExecutionChain) != 2 || job2.ExecutionChain[0] != job1.ID || job2.ExecutionChain[1] != job2.ID {
		t.Fatalf("expected job2 execution chain [%s, %s], got %+v", job1.ID, job2.ID, job2.ExecutionChain)
	}

	// 4. Verify Schedule Detail API (/v1/schedules/{id}) includes all associated jobs
	resp, err = http.Get(ts.URL + "/v1/schedules/" + sch1.ID)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get schedule detail: %v, status: %v", err, resp.StatusCode)
	}
	var schDetail map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&schDetail)
	assocJobs, ok := schDetail["associated_jobs"].([]any)
	if !ok || len(assocJobs) < 2 {
		t.Fatalf("expected at least 2 associated jobs in schedule detail, got %+v", schDetail["associated_jobs"])
	}

	// 5. Confirm same conversation attach via HTTP and verify exit 0 confirms same_conversation
	conv2Payload := []byte(`{"conversation_id":"` + initialConvID + `","role":"initial_resumed","cli_args":"--conversation=` + initialConvID + `"}`)
	resp, err = http.Post(ts.URL+"/v1/jobs/"+job2.ID+"/conversations", "application/json", bytes.NewReader(conv2Payload))
	if err != nil || resp.StatusCode >= 300 {
		t.Fatalf("failed to post conversation for job2: %v, status: %v", err, resp.StatusCode)
	}

	exit0Payload := []byte(`{"exit_code":0}`)
	resp, err = http.Post(ts.URL+"/v1/jobs/"+job2.ID+"/exit", "application/json", bytes.NewReader(exit0Payload))
	if err != nil || resp.StatusCode >= 300 {
		t.Fatalf("failed to post exit 0 for job2: %v, status: %v", err, resp.StatusCode)
	}

	// 6. Verify history HTTP endpoint returns confirmed same_conversation status
	resp, err = http.Get(ts.URL + "/v1/history?host=mac-mini&path=" + repoPath)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get repo history: %v, status: %v", err, resp.StatusCode)
	}
	var repoDetail map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&repoDetail)
	history, ok := repoDetail["history"].([]any)
	if !ok || len(history) < 2 {
		t.Fatalf("expected at least 2 history entries, got %+v", repoDetail["history"])
	}

	// Verify metrics endpoint (/v1/observability/metrics) contains resume metrics
	resp, err = http.Get(ts.URL + "/v1/observability/metrics")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get metrics: %v, status: %v", err, resp.StatusCode)
	}
	var metrics map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&metrics)
	resMetrics, ok := metrics["resume_metrics"].(map[string]any)
	if !ok {
		t.Fatalf("expected resume_metrics in metrics response, got %+v", metrics)
	}
	histRows := eng.ListHistory("", "", "", 0)
	for _, hr := range histRows {
		t.Logf("HIST ROW: JobID=%s Status=%s ResumeMode=%s SameConfirmed=%v AttachConf=%s", hr.JobID, hr.Status, hr.ResumeMode, hr.SameConversationConfirmed, hr.AttachConfirmation)
	}
	if resMetrics["same_conversation_success_total"].(float64) < 1 {
		t.Fatalf("expected same_conversation_success_total >= 1, got %v", resMetrics["same_conversation_success_total"])
	}
}

func TestE2E_ResumeControlledFallback_AndMetrics(t *testing.T) {
	eng := control.NewEngine(nil)
	srv := &httpserver.Server{Engine: eng}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	eng.RegisterHost("mac-mini", "permanent")
	repoPath := "/Users/romeo/work/repo-03"
	_ = eng.UpsertRepo(control.Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        control.QueueOpen,
		Lock:         control.LockIdle,
	})

	initialConvID := "aad902c8-bda6-44c7-8a68-c468a1562399"
	fallbackConvID := "1d446877-5bf9-44eb-9db5-e1c007a9fe31"

	p := eng.PutPrompt(control.Prompt{
		ID:     "prompt-ase-fallback",
		Title:  "Autonomous Story Execution Fallback",
		Engine: "agy",
		Status: "READY",
	})
	_, err := eng.ExecutePromptWithResume(p.ID, "mac-mini", repoPath, 1, nil, initialConvID)
	if err != nil {
		t.Fatalf("failed to execute prompt: %v", err)
	}

	job1, _, _, ok := eng.PollHost("mac-mini")
	if !ok {
		t.Fatal("expected job1 on poll")
	}

	// Post exit 1
	_ = eng.PostExit(job1.ID, 1, control.ExitOptions{Reason: "FAILED"})

	// Resume repo in place
	resumeReq := []byte(`{"path":"` + repoPath + `","engine":"agy"}`)
	resp, err := http.Post(ts.URL+"/v1/repos/mac-mini/resume-in-place", "application/json", bytes.NewReader(resumeReq))
	if err != nil || resp.StatusCode >= 300 {
		t.Fatalf("failed to resume repo in place: %v, status: %v", err, resp.StatusCode)
	}

	job2, _, _, ok := eng.PollHost("mac-mini")
	if !ok {
		t.Fatal("expected job2 on poll after resume")
	}

	// Simulate Stage 2 controlled fallback via HTTP /conversations endpoint
	fbPayload := []byte(`{
		"conversation_id": "` + fallbackConvID + `",
		"fallback": true,
		"fallback_reason": "session corrupted on host disk",
		"role": "fallback_primary",
		"cli_args": "--conversation=` + fallbackConvID + `"
	}`)
	resp, err = http.Post(ts.URL+"/v1/jobs/"+job2.ID+"/conversations", "application/json", bytes.NewReader(fbPayload))
	if err != nil || resp.StatusCode >= 300 {
		t.Fatalf("failed to post fallback conversation: %v, status: %v", err, resp.StatusCode)
	}

	// Exit 0 on job 2
	resp, err = http.Post(ts.URL+"/v1/jobs/"+job2.ID+"/exit", "application/json", bytes.NewReader([]byte(`{"exit_code":0}`)))
	if err != nil || resp.StatusCode >= 300 {
		t.Fatalf("failed to post exit 0: %v, status: %v", err, resp.StatusCode)
	}

	// Check metrics
	resp, err = http.Get(ts.URL + "/v1/observability/metrics")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get metrics: %v, status: %v", err, resp.StatusCode)
	}
	var metrics map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&metrics)
	resMetrics, ok := metrics["resume_metrics"].(map[string]any)
	if !ok {
		t.Fatalf("expected resume_metrics in metrics response, got %+v", metrics)
	}
	if resMetrics["fallback_new_conversation_total"].(float64) < 1 {
		t.Fatalf("expected fallback_new_conversation_total >= 1, got %v", resMetrics["fallback_new_conversation_total"])
	}
}

func TestE2E_ResumeJob_HistoryOnly_BindsParentAndActiveConversation(t *testing.T) {
	eng := control.NewEngine(nil)
	srv := &httpserver.Server{Engine: eng}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	eng.RegisterHost("mac-mini", "permanent")
	repoPath := "/Users/romeo/work/repo-03"
	_ = eng.UpsertRepo(control.Repo{
		HostID:       "mac-mini",
		WorktreePath: repoPath,
		Queue:        control.QueueOpen,
		Lock:         control.LockIdle,
	})

	initialConvID := "aad902c8-bda6-44c7-8a68-c468a1562399"
	leakedConvID := "14de9a1f-5679-4f11-a08f-f32bc7e47663"

	p := eng.PutPrompt(control.Prompt{
		ID:     "prompt-hist-test",
		Title:  "History Test STORY-GOV-3678",
		Engine: "agy",
		Status: "READY",
	})
	sch1, err := eng.ExecutePromptWithResume(p.ID, "mac-mini", repoPath, 1, nil, initialConvID)
	if err != nil {
		t.Fatalf("failed to execute prompt: %v", err)
	}

	job1, _, _, ok := eng.PollHost("mac-mini")
	if !ok {
		t.Fatal("expected job1 on poll")
	}

	// Post initial conversation
	conv1Payload := []byte(`{"conversation_id":"` + initialConvID + `","role":"initial_active","cli_args":"--conversation=` + initialConvID + `"}`)
	resp, err := http.Post(ts.URL+"/v1/jobs/"+job1.ID+"/conversations", "application/json", bytes.NewReader(conv1Payload))
	if err != nil || resp.StatusCode >= 300 {
		t.Fatalf("failed to post conversation 1: %v", err)
	}

	// Post dropped tertiary conversation (leaked ID)
	leakedPayload := []byte(`{"conversation_id":"` + leakedConvID + `","role":"dropped_unpinned","cli_args":""}`)
	resp, err = http.Post(ts.URL+"/v1/jobs/"+job1.ID+"/conversations", "application/json", bytes.NewReader(leakedPayload))
	if err != nil || resp.StatusCode >= 300 {
		t.Fatalf("failed to post leaked conversation: %v", err)
	}

	// Exit 1
	resp, err = http.Post(ts.URL+"/v1/jobs/"+job1.ID+"/exit", "application/json", bytes.NewReader([]byte(`{"exit_code":1}`)))
	if err != nil || resp.StatusCode >= 300 {
		t.Fatalf("failed to post exit 1: %v", err)
	}

	// Purge active in-memory jobs and schedule to simulate API restart / memory reaping (C1)
	eng.PurgeJobsForTest()

	// Call POST /v1/jobs/{id}/resume with attempt to override parent_job_id and inject step (C5)
	overrideBody := []byte(`{
		"parent_job_id": "job-attacker-override",
		"initial_conversation_id": "bad-bad-bad",
		"interrupted_ase_state": {"step": "exploit"}
	}`)
	resp, err = http.Post(ts.URL+"/v1/jobs/"+job1.ID+"/resume", "application/json", bytes.NewReader(overrideBody))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to resume job from history: %v, status: %v", err, resp.StatusCode)
	}

	var resSch map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&resSch)

	// Poll host to inspect resumed job2
	job2, _, _, ok := eng.PollHost("mac-mini")
	if !ok {
		t.Fatal("expected job2 on poll after history resume")
	}

	// C1 Verification: must pin initialConvID, not leakedConvID
	if job2.InitialConversationID != initialConvID {
		t.Fatalf("expected job2 InitialConversationID=%s, got %s", initialConvID, job2.InitialConversationID)
	}
	if job2.ResumeConversationID != initialConvID {
		t.Fatalf("expected job2 ResumeConversationID=%s, got %s", initialConvID, job2.ResumeConversationID)
	}

	// C5 Verification: server authoritatively derived ParentJobID and ignored client override
	if job2.ParentJobID != job1.ID {
		t.Fatalf("expected job2 ParentJobID=%s, got %s", job1.ID, job2.ParentJobID)
	}

	// Verify schedule associated jobs includes job1 and job2
	resp, err = http.Get(ts.URL + "/v1/schedules/" + sch1.ID)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("failed to get schedule detail: %v", err)
	}
	var schDetail map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&schDetail)
	assocJobs, _ := schDetail["associated_jobs"].([]any)
	if len(assocJobs) < 2 {
		t.Fatalf("expected at least 2 associated jobs, got %+v", assocJobs)
	}
}
