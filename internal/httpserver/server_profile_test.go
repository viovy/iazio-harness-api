package httpserver

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/viovy/iazio-harness-api/internal/control"
)

func TestServer_ProfileRoutes(t *testing.T) {
	e := control.NewEngine(time.Now)
	srv := &Server{Engine: e}
	handler := srv.Handler()

	// 1. Sync generic profile from GitOps payload
	syncPayload := `
fleet_distribution:
  schema_version: 1
  install_root:
    unix:
      primary: ~/.local/bin
      fallback: ~/.iazio/bin
    windows:
      primary: "%USERPROFILE%/bin"
      fallback: "%USERPROFILE%/.iazio/bin"
  endpoints:
    harness_api: "https://example.com/iazio-harness-api"
    iazio_api: "https://example.com/iazio-api"
  repositories:
    autopilot-core:
      repo: viovy/autopilot
      type: github_release
      is_private: true
  tools:
    - name: autopilot
      repository: autopilot-core
      binary_name: autopilot
      description: Autopilot CLI orchestration harness
      tier: baseline
`
	reqSync := httptest.NewRequest("POST", "/v1/fleet/profiles/generic/sync", bytes.NewReader([]byte(syncPayload)))
	recSync := httptest.NewRecorder()
	handler.ServeHTTP(recSync, reqSync)
	if recSync.Code != http.StatusOK {
		t.Fatalf("expected 200 on sync, got %d: %s", recSync.Code, recSync.Body.String())
	}

	// 2. List profiles
	reqList := httptest.NewRequest("GET", "/v1/fleet/profiles", nil)
	recList := httptest.NewRecorder()
	handler.ServeHTTP(recList, reqList)
	if recList.Code != http.StatusOK {
		t.Fatalf("expected 200 on list, got %d", recList.Code)
	}
	var profiles []control.DistributionProfile
	if err := json.Unmarshal(recList.Body.Bytes(), &profiles); err != nil {
		t.Fatalf("unmarshal profiles: %v", err)
	}
	if len(profiles) != 1 || profiles[0].Name != "generic" {
		t.Fatalf("expected generic profile in list, got %+v", profiles)
	}

	// 3. Create a custom profile
	customProfile := control.DistributionProfile{
		Name:        "ci-runner",
		Description: "CI Runner Profile",
		Tools: []control.ProfileTool{
			{Name: "autopilot", Repository: "viovy/autopilot", BinaryName: "autopilot", Tier: "baseline"},
		},
	}
	customBytes, _ := json.Marshal(customProfile)
	reqCreate := httptest.NewRequest("POST", "/v1/fleet/profiles", bytes.NewReader(customBytes))
	recCreate := httptest.NewRecorder()
	handler.ServeHTTP(recCreate, reqCreate)
	if recCreate.Code != http.StatusCreated {
		t.Fatalf("expected 201 on create, got %d: %s", recCreate.Code, recCreate.Body.String())
	}

	// 4. Get profile
	reqGet := httptest.NewRequest("GET", "/v1/fleet/profiles/ci-runner", nil)
	recGet := httptest.NewRecorder()
	handler.ServeHTTP(recGet, reqGet)
	if recGet.Code != http.StatusOK {
		t.Fatalf("expected 200 on get, got %d", recGet.Code)
	}

	// 5. Delete generic is forbidden
	reqDelGeneric := httptest.NewRequest("DELETE", "/v1/fleet/profiles/generic", nil)
	recDelGeneric := httptest.NewRecorder()
	handler.ServeHTTP(recDelGeneric, reqDelGeneric)
	if recDelGeneric.Code != http.StatusForbidden {
		t.Fatalf("expected 403 on deleting generic, got %d", recDelGeneric.Code)
	}

	// 6. Delete custom profile succeeds
	reqDelCustom := httptest.NewRequest("DELETE", "/v1/fleet/profiles/ci-runner", nil)
	recDelCustom := httptest.NewRecorder()
	handler.ServeHTTP(recDelCustom, reqDelCustom)
	if recDelCustom.Code != http.StatusOK {
		t.Fatalf("expected 200 on delete custom, got %d", recDelCustom.Code)
	}

	// 7. Get Manifest for Windows
	reqManifest := httptest.NewRequest("GET", "/v1/fleet/manifest?profile=generic&os=windows", nil)
	recManifest := httptest.NewRecorder()
	handler.ServeHTTP(recManifest, reqManifest)
	if recManifest.Code != http.StatusOK {
		t.Fatalf("expected 200 on manifest, got %d", recManifest.Code)
	}
	var manifest control.ManifestResponse
	if err := json.Unmarshal(recManifest.Body.Bytes(), &manifest); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if len(manifest.Tools) != 1 || manifest.Tools[0].BinaryName != "autopilot.exe" {
		t.Errorf("expected autopilot.exe on Windows manifest, got %+v", manifest.Tools)
	}
	if manifest.Endpoints == nil || manifest.Endpoints["harness_api"] != "https://example.com/iazio-harness-api" {
		t.Errorf("expected endpoints in manifest, got %+v", manifest.Endpoints)
	}

	// 8. Register host with profile
	regPayload := `{"id": "node-1", "kind": "permanent", "profile": "generic"}`
	reqReg := httptest.NewRequest("POST", "/v1/hosts/register", bytes.NewReader([]byte(regPayload)))
	recReg := httptest.NewRecorder()
	handler.ServeHTTP(recReg, reqReg)
	if recReg.Code != http.StatusOK {
		t.Fatalf("expected 200 on register, got %d", recReg.Code)
	}
	var host control.Host
	if err := json.Unmarshal(recReg.Body.Bytes(), &host); err != nil {
		t.Fatalf("unmarshal host: %v", err)
	}
	if host.DistributionProfile != "generic" {
		t.Errorf("expected host distribution profile generic, got %s", host.DistributionProfile)
	}
}
