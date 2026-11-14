package dispatch

import (
	"errors"
	"strings"
)

// PrematureCraftCloseoutMessage is the remediable ASE gate error injected when a
// completed/success result still contains mid-flow CRAFT / land-wait prose
// (STORY-IAZIO-0204 RC-7).
const PrematureCraftCloseoutMessage = "ASE_GATE_INCOMPLETE: premature_craft_closeout"

// PrematureCraftCloseoutError is the sentinel error matching PrematureCraftCloseoutMessage.
var PrematureCraftCloseoutError = errors.New(PrematureCraftCloseoutMessage)

var prematureCraftMidFlowMarkers = []string{
	"waiting for",
	"will wait for",
	"awaiting completion",
	"launched `autopilot-git-flow",
	"launched autopilot-git-flow",
	"launched `autopilot git-flow",
}

var prematureCraftTerminalAssertProofMarkers = []string{
	"assert-git-flow-complete",
	"git-flow assert",
	"review-scheme assert",
	"autonomous story execution is complete",
}

// IsPrematureCraftCloseout reports whether combined summary/output text shows a
// premature CRAFT or mid-flow "completed" closeout: mid-flow wait markers plus
// either a CRAFT Closeout headline or a success claim lacking terminal assert proof.
func IsPrematureCraftCloseout(summary, output string) bool {
	combined := strings.TrimSpace(summary) + "\n" + strings.TrimSpace(output)
	if strings.TrimSpace(combined) == "" {
		return false
	}
	lower := strings.ToLower(combined)
	if !hasPrematureCraftMidFlowMarker(lower) {
		return false
	}
	if strings.Contains(lower, "## craft closeout") || strings.Contains(lower, "# craft closeout") {
		return true
	}
	// Mid-flow-only completed/success results (no CRAFT) are premature unless
	// terminal assert proof language is present.
	return !hasPrematureCraftTerminalAssertProof(lower)
}

func hasPrematureCraftMidFlowMarker(lower string) bool {
	for _, marker := range prematureCraftMidFlowMarkers {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func hasPrematureCraftTerminalAssertProof(lower string) bool {
	for _, marker := range prematureCraftTerminalAssertProofMarkers {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}
