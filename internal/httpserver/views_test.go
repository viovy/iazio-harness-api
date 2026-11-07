package httpserver

import (
	"strings"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/control"
	"github.com/viovy/iazio-harness-api/internal/dispatch"
)

func TestStory0194_ViewsEmitCanonicalJobsLiveURL(t *testing.T) {
	now := time.Date(2026, 11, 7, 8, 50, 0, 0, time.UTC)

	// 1. scheduleView
	sv := scheduleView(control.Schedule{
		ID:           "sch-3a5ffef87ff013f2",
		HostID:       "debian",
		WorktreePath: "/home/romeo/work/meta-repo",
	})
	if sv["live_url"] != "https://tian.go.ro/iazio-web/jobs/sch-3a5ffef87ff013f2" {
		t.Fatalf("expected canonical live_url on scheduleView, got %v", sv["live_url"])
	}

	// 2. repoDetailView running job
	rdv := repoDetailView(control.RepoDetail{
		Repo: control.Repo{
			HostID:       "debian",
			WorktreePath: "/home/romeo/work/meta-repo",
		},
		Running: &control.Job{
			ID:     "job_3a5ffef87ff013f2",
			Status: "RUNNING",
			HostID: "debian",
		},
	})
	runningMap, ok := rdv["running_job"].(map[string]any)
	if !ok || runningMap == nil {
		t.Fatalf("expected running_job map in repoDetailView")
	}
	if runningMap["live_url"] != "https://tian.go.ro/iazio-web/jobs/job_3a5ffef87ff013f2" {
		t.Fatalf("expected canonical live_url on repoDetailView.running_job, got %v", runningMap["live_url"])
	}

	// 3. historyRowView
	hrv := historyRowView(control.HistoryRow{
		JobID:     "job_hist_0194",
		CreatedAt: now,
	})
	if hrv["live_url"] != "https://tian.go.ro/iazio-web/jobs/job_hist_0194" {
		t.Fatalf("expected canonical live_url on historyRowView, got %v", hrv["live_url"])
	}

	// 4. jobDetailView
	jdv := jobDetailView(control.JobDetail{
		ID:           "job_detail_0194",
		HostID:       "debian",
		WorktreePath: "/home/romeo/work/meta-repo",
	})
	if jdv["live_url"] != "https://tian.go.ro/iazio-web/jobs/job_detail_0194" {
		t.Fatalf("expected canonical live_url on jobDetailView, got %v", jdv["live_url"])
	}

	// 5. dispatchScheduleView, dispatchHistoryView, dispatchJobDetailView
	dJob := &dispatch.Job{
		ID:          "job_3a5ffef87ff013f2",
		Type:        dispatch.JobTypePromptExecute,
		Repo:        "/home/romeo/work/meta-repo",
		ClaimedHost: "debian",
		HostID:      "debian",
		Status:      dispatch.JobStatusRunning,
		CreatedAt:   now,
	}
	for name, m := range map[string]map[string]any{
		"dispatchScheduleView":  dispatchScheduleView(dJob),
		"dispatchHistoryView":   dispatchHistoryView(dJob),
		"dispatchJobDetailView": dispatchJobDetailView(dJob),
	} {
		got, _ := m["live_url"].(string)
		if got != "https://tian.go.ro/iazio-web/jobs/job_3a5ffef87ff013f2" {
			t.Fatalf("%s live_url = %q, want https://tian.go.ro/iazio-web/jobs/job_3a5ffef87ff013f2", name, got)
		}
		if strings.Contains(got, "/root/live") || strings.Contains(got, "/home/romeo") || strings.Contains(got, "/agents/") {
			t.Fatalf("%s live_url leaked invalid path: %q", name, got)
		}
	}
}
