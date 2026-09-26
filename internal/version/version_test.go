package version

import "testing"

func TestInformational(t *testing.T) {
	Version, Commit = "0.1.0-dev", "unknown"
	if Informational() != "0.1.0-dev" {
		t.Fatal(Informational())
	}
	Version, Commit = "1.2.3", "abcdef1234567890"
	if Informational() != "1.2.3+abcdef123456" {
		t.Fatal(Informational())
	}
}
