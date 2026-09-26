// Package version reports the linked build identity.
package version

import "strings"

var (
	// Version is the SemVer stamped at link time.
	Version = "0.1.0-dev"
	// Commit is the source revision stamped at link time.
	Commit = "unknown"
	// Branch is the branch stamped at link time.
	Branch = "unknown"
)

// Informational returns FullSemVer+shortSHA when a commit was stamped.
func Informational() string {
	if Commit == "" || Commit == "unknown" {
		return Version
	}
	sha := Commit
	if len(sha) > 12 {
		sha = sha[:12]
	}
	if strings.Contains(Version, "+") {
		return Version
	}
	return Version + "+" + sha
}
