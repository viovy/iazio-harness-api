package gitlease

import (
	"testing"
	"time"
)

func TestGitLeaseManager_AcquireAndRelease(t *testing.T) {
	mgr := NewManager()

	lease, acquired, err := mgr.Acquire("viovy/meta-repo", "workspace-1", 60)
	if err != nil {
		t.Fatalf("unexpected acquire error: %v", err)
	}
	if !acquired {
		t.Fatalf("expected lease to be acquired")
	}
	if lease.Repo != "viovy/meta-repo" || lease.Holder != "workspace-1" {
		t.Errorf("unexpected lease fields: %+v", lease)
	}

	// Second holder trying to acquire same repo should fail
	conflict, acquired2, err := mgr.Acquire("viovy/meta-repo", "workspace-2", 60)
	if err != nil {
		t.Fatalf("unexpected conflict acquire error: %v", err)
	}
	if acquired2 {
		t.Fatalf("expected acquired2 to be false due to active lease")
	}
	if conflict.Holder != "workspace-1" {
		t.Errorf("expected conflict to point to workspace-1, got %s", conflict.Holder)
	}

	// Release with wrong leaseID should fail
	ok, err := mgr.Release("viovy/meta-repo", "workspace-1", "bad-id")
	if err != ErrLeaseIDMismatch || ok {
		t.Errorf("expected ErrLeaseIDMismatch, got ok=%v err=%v", ok, err)
	}

	// Release with correct leaseID should succeed
	ok, err = mgr.Release("viovy/meta-repo", "workspace-1", lease.LeaseID)
	if err != nil || !ok {
		t.Fatalf("expected successful release: ok=%v err=%v", ok, err)
	}

	// Now workspace-2 can acquire
	lease2, acquired3, err := mgr.Acquire("viovy/meta-repo", "workspace-2", 30)
	if err != nil || !acquired3 {
		t.Fatalf("expected workspace-2 to acquire: acquired=%v err=%v", acquired3, err)
	}
	if lease2.Holder != "workspace-2" {
		t.Errorf("unexpected holder: %s", lease2.Holder)
	}
}

func TestGitLeaseManager_TTLAndExpiration(t *testing.T) {
	mgr := NewManager()
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	mgr.nowFn = func() time.Time { return now }

	lease, acquired, err := mgr.Acquire("iazio-agent", "holder-a", 10)
	if err != nil || !acquired {
		t.Fatalf("failed initial acquire: %v", err)
	}

	// Advance time past TTL
	now = now.Add(15 * time.Second)

	// holder-b should be able to acquire because lease expired
	leaseB, acquiredB, err := mgr.Acquire("iazio-agent", "holder-b", 20)
	if err != nil || !acquiredB {
		t.Fatalf("expected holder-b to acquire expired lease: acquired=%v, err=%v", acquiredB, err)
	}
	if leaseB.Holder != "holder-b" {
		t.Errorf("expected holder-b, got %s", leaseB.Holder)
	}
	if leaseB.LeaseID == lease.LeaseID {
		t.Errorf("expected new lease ID after expiration")
	}
}

func TestGitLeaseManager_Renew(t *testing.T) {
	mgr := NewManager()
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	mgr.nowFn = func() time.Time { return now }

	lease, acquired, err := mgr.Acquire("iazio-web", "holder-c", 30)
	if err != nil || !acquired {
		t.Fatalf("failed acquire: %v", err)
	}
	initialExpiry := lease.ExpiresAt

	// Advance 10s and renew with 30s
	now = now.Add(10 * time.Second)
	renewed, ok, err := mgr.Renew("iazio-web", "holder-c", lease.LeaseID, 30)
	if err != nil || !ok {
		t.Fatalf("failed renew: ok=%v, err=%v", ok, err)
	}
	if !renewed.ExpiresAt.After(initialExpiry) {
		t.Errorf("expected extended expiry: %v vs %v", renewed.ExpiresAt, initialExpiry)
	}
}

func TestGitLeaseManager_Normalization(t *testing.T) {
	mgr := NewManager()

	lease, acquired, err := mgr.Acquire("/Viovy/Meta-Repo.git/", "holder-d", 60)
	if err != nil || !acquired {
		t.Fatalf("acquire error: %v", err)
	}

	// Check with different casing and formatting
	found := mgr.Get("viovy/meta-repo")
	if found == nil || found.LeaseID != lease.LeaseID {
		t.Errorf("normalization lookup failed: found=%+v", found)
	}
}
