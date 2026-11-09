package dispatch

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Canonical RunComparison diff bucket identifiers.
const (
	BucketResolvedErrors   = "RESOLVED_ERRORS"
	BucketNewRegressions   = "NEW_REGRESSIONS"
	BucketPersistentErrors = "PERSISTENT_ERRORS"
	BucketLatencyDeltas    = "LATENCY_DELTAS"
)

// Canonical RunComparison remediation verdicts.
const (
	VerdictRemediationVerified  = "REMEDIATION_VERIFIED"
	VerdictRegressionIntroduced = "REGRESSION_INTRODUCED"
	VerdictPersistentFailure    = "PERSISTENT_FAILURE"
	VerdictCleanRun             = "CLEAN_RUN"
)

// DefaultLatencyRegressionThresholdPct is the percentage increase at which a span or run duration is flagged as regressed.
const DefaultLatencyRegressionThresholdPct = 20.0

// DefaultLatencyRegressionMinDeltaMs is the minimum absolute millisecond increase required to flag a latency regression.
const DefaultLatencyRegressionMinDeltaMs = 25.0

// ErrorClusterItem represents a normalized error or warning cluster signature in a telemetry snapshot.
type ErrorClusterItem struct {
	Signature     string `json:"signature"`
	Level         string `json:"level,omitempty"`
	Service       string `json:"service,omitempty"`
	Category      string `json:"category,omitempty"` // error_cluster, span_error_tag, test_failure, warning_signature, job_error
	Count         int    `json:"count"`
	SampleMessage string `json:"sample_message,omitempty"`
}

// SpanLatencyItem captures Zipkin/OTel span latency and error tag telemetry for a single span signature.
type SpanLatencyItem struct {
	SpanName      string  `json:"span_name"`
	ServiceName   string  `json:"service_name,omitempty"`
	DurationMs    float64 `json:"duration_ms"`
	P95DurationMs float64 `json:"p95_duration_ms,omitempty"`
	SpanCount     int     `json:"span_count,omitempty"`
	HasError      bool    `json:"has_error,omitempty"`
	ErrorTag      string  `json:"error_tag,omitempty"`
	StatusCode    string  `json:"status_code,omitempty"`
}

// RunTelemetrySnapshot captures behavioral and telemetry metrics for a single job execution, retry attempt, or time window.
type RunTelemetrySnapshot struct {
	RunID             string             `json:"run_id"`
	JobID             string             `json:"job_id,omitempty"`
	StoryID           string             `json:"story_id,omitempty"`
	AttemptNumber     int                `json:"attempt_number,omitempty"`
	CommitSHA         string             `json:"commit_sha,omitempty"`
	Status            string             `json:"status,omitempty"`
	WindowLabel       string             `json:"window_label,omitempty"`
	StartedAt         string             `json:"started_at,omitempty"`
	CompletedAt       string             `json:"completed_at,omitempty"`
	DurationMs        float64            `json:"duration_ms,omitempty"`
	TurnCount         int                `json:"turn_count,omitempty"`
	TokenCount        int                `json:"token_count,omitempty"`
	TestFailureCount  int                `json:"test_failure_count"`
	TestPassCount     int                `json:"test_pass_count,omitempty"`
	FailedTests       []string           `json:"failed_tests,omitempty"`
	ErrorClusters     []ErrorClusterItem `json:"error_clusters,omitempty"`
	WarningSignatures []string           `json:"warning_signatures,omitempty"`
	SpanLatencies     []SpanLatencyItem  `json:"span_latencies,omitempty"`
	RawError          string             `json:"raw_error,omitempty"`
}

// ErrorDiffEntry describes a single error signature, span error tag, warning signature, or test failure diff between baseline and current runs.
type ErrorDiffEntry struct {
	Signature     string `json:"signature"`
	Bucket        string `json:"bucket"` // RESOLVED_ERRORS, NEW_REGRESSIONS, PERSISTENT_ERRORS
	Category      string `json:"category,omitempty"`
	Service       string `json:"service,omitempty"`
	Level         string `json:"level,omitempty"`
	BaselineCount int    `json:"baseline_count"`
	CurrentCount  int    `json:"current_count"`
	DeltaCount    int    `json:"delta_count"`
	SampleMessage string `json:"sample_message,omitempty"`
}

// LatencyDeltaEntry describes the duration or efficiency delta for a Zipkin span or overall execution metric.
type LatencyDeltaEntry struct {
	SpanName      string  `json:"span_name"`
	ServiceName   string  `json:"service_name,omitempty"`
	MetricType    string  `json:"metric_type,omitempty"` // span_latency, job_duration
	BaselineMs    float64 `json:"baseline_ms"`
	CurrentMs     float64 `json:"current_ms"`
	DeltaMs       float64 `json:"delta_ms"`
	DeltaPct      float64 `json:"delta_pct"`
	BaselineError bool    `json:"baseline_error,omitempty"`
	CurrentError  bool    `json:"current_error,omitempty"`
	ErrorTag      string  `json:"error_tag,omitempty"`
	Regressed     bool    `json:"regressed"`
}

// TestFailureDiffSummary captures run-to-run test failure count and individual test suite diffs.
type TestFailureDiffSummary struct {
	BaselineFailures int      `json:"baseline_failures"`
	CurrentFailures  int      `json:"current_failures"`
	Delta            int      `json:"delta"`
	ResolvedTests    []string `json:"resolved_tests,omitempty"`
	NewFailedTests   []string `json:"new_failed_tests,omitempty"`
	PersistentTests  []string `json:"persistent_tests,omitempty"`
}

// EfficiencyDeltaSummary captures turn, token, and wall-clock duration deltas across runs.
type EfficiencyDeltaSummary struct {
	BaselineTurns      int     `json:"baseline_turns"`
	CurrentTurns       int     `json:"current_turns"`
	DeltaTurns         int     `json:"delta_turns"`
	BaselineTokens     int     `json:"baseline_tokens"`
	CurrentTokens      int     `json:"current_tokens"`
	DeltaTokens        int     `json:"delta_tokens"`
	BaselineDurationMs float64 `json:"baseline_duration_ms"`
	CurrentDurationMs  float64 `json:"current_duration_ms"`
	DeltaDurationMs    float64 `json:"delta_duration_ms"`
}

// RunComparisonBuckets exposes the four canonical uppercase bucket names directly in JSON.
type RunComparisonBuckets struct {
	ResolvedErrors   []ErrorDiffEntry    `json:"RESOLVED_ERRORS"`
	NewRegressions   []ErrorDiffEntry    `json:"NEW_REGRESSIONS"`
	PersistentErrors []ErrorDiffEntry    `json:"PERSISTENT_ERRORS"`
	LatencyDeltas    []LatencyDeltaEntry `json:"LATENCY_DELTAS"`
}

// RunComparisonDiff is the deterministic run-to-run behavioral and telemetry comparison output.
type RunComparisonDiff struct {
	BaselineRunID       string                 `json:"baseline_run_id"`
	CurrentRunID        string                 `json:"current_run_id"`
	StoryID             string                 `json:"story_id,omitempty"`
	ComparisonMode      string                 `json:"comparison_mode,omitempty"`
	Verdict             string                 `json:"verdict"`
	RemediationVerified bool                   `json:"remediation_verified"`
	ResolvedErrors      []ErrorDiffEntry       `json:"resolved_errors"`
	NewRegressions      []ErrorDiffEntry       `json:"new_regressions"`
	PersistentErrors    []ErrorDiffEntry       `json:"persistent_errors"`
	LatencyDeltas       []LatencyDeltaEntry    `json:"latency_deltas"`
	Buckets             RunComparisonBuckets   `json:"buckets"`
	TestFailureDelta    TestFailureDiffSummary `json:"test_failure_delta"`
	EfficiencyDelta     EfficiencyDeltaSummary `json:"efficiency_delta"`
	Summary             string                 `json:"summary"`
	Phase4ReviewContext string                 `json:"phase4_review_context"`
	ComparedAt          string                 `json:"compared_at"`
}

// RunComparisonService computes deterministic behavioral and telemetry diffs between job executions or telemetry windows.
type RunComparisonService struct {
	LatencyRegressionThresholdPct float64
	LatencyRegressionMinDeltaMs   float64
}

// NewRunComparisonService returns a RunComparisonService configured with default regression thresholds.
func NewRunComparisonService() *RunComparisonService {
	return &RunComparisonService{
		LatencyRegressionThresholdPct: DefaultLatencyRegressionThresholdPct,
		LatencyRegressionMinDeltaMs:   DefaultLatencyRegressionMinDeltaMs,
	}
}

var (
	hexTokenRe  = regexp.MustCompile(`\b[0-9a-fA-F]{8,64}\b`)
	numTokenRe  = regexp.MustCompile(`\b\d{4,}\b`)
	goTestFail  = regexp.MustCompile(`(?m)^--- FAIL:\s+([A-Za-z0-9_/-]+)`)
	wsCollapse  = regexp.MustCompile(`\s+`)
)

// NormalizeErrorSignature canonicalizes a raw error message or cluster signature for deterministic comparison.
func NormalizeErrorSignature(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	if idx := strings.IndexByte(s, '\n'); idx > 0 {
		s = strings.TrimSpace(s[:idx])
	}
	s = hexTokenRe.ReplaceAllString(s, "<hex>")
	s = numTokenRe.ReplaceAllString(s, "<num>")
	s = wsCollapse.ReplaceAllString(s, " ")
	if len(s) > 180 {
		s = s[:180]
	}
	return s
}

// CompareRuns compares a baseline and current RunTelemetrySnapshot and classifies diffs into
// RESOLVED_ERRORS, NEW_REGRESSIONS, PERSISTENT_ERRORS, and LATENCY_DELTAS.
func (s *RunComparisonService) CompareRuns(baseline, current RunTelemetrySnapshot) *RunComparisonDiff {
	if s == nil {
		s = NewRunComparisonService()
	}
	thresholdPct := s.LatencyRegressionThresholdPct
	if thresholdPct <= 0 {
		thresholdPct = DefaultLatencyRegressionThresholdPct
	}
	minDeltaMs := s.LatencyRegressionMinDeltaMs
	if minDeltaMs <= 0 {
		minDeltaMs = DefaultLatencyRegressionMinDeltaMs
	}

	resolved := make([]ErrorDiffEntry, 0)
	regressions := make([]ErrorDiffEntry, 0)
	persistent := make([]ErrorDiffEntry, 0)
	latencyDeltas := make([]LatencyDeltaEntry, 0)

	type aggCluster struct {
		signature string
		category  string
		service   string
		level     string
		count     int
		sample    string
	}

	buildClusterMap := func(snap RunTelemetrySnapshot) map[string]*aggCluster {
		m := make(map[string]*aggCluster)
		add := func(sig, category, service, level, sample string, count int) {
			norm := NormalizeErrorSignature(sig)
			if norm == "" {
				return
			}
			if count <= 0 {
				count = 1
			}
			if category == "" {
				category = "error_cluster"
			}
			if level == "" {
				level = "ERROR"
			}
			key := category + "|" + strings.ToLower(service) + "|" + norm
			if existing, ok := m[key]; ok {
				existing.count += count
				if existing.sample == "" && sample != "" {
					existing.sample = sample
				}
				return
			}
			m[key] = &aggCluster{
				signature: norm,
				category:  category,
				service:   service,
				level:     level,
				count:     count,
				sample:    sample,
			}
		}

		for _, c := range snap.ErrorClusters {
			cat := c.Category
			if cat == "" {
				if strings.EqualFold(c.Level, "WARN") || strings.EqualFold(c.Level, "WARNING") {
					cat = "warning_signature"
				} else {
					cat = "error_cluster"
				}
			}
			add(c.Signature, cat, c.Service, c.Level, c.SampleMessage, c.Count)
		}
		for _, w := range snap.WarningSignatures {
			add(w, "warning_signature", "", "WARN", w, 1)
		}
		for _, sp := range snap.SpanLatencies {
			if sp.HasError || strings.TrimSpace(sp.ErrorTag) != "" || strings.HasPrefix(strings.TrimSpace(sp.StatusCode), "5") {
				errTag := strings.TrimSpace(sp.ErrorTag)
				if errTag == "" {
					if sp.StatusCode != "" {
						errTag = "http.status_code=" + sp.StatusCode
					} else {
						errTag = "error=true"
					}
				}
				sig := fmt.Sprintf("span:%s (%s)", sp.SpanName, errTag)
				add(sig, "span_error_tag", sp.ServiceName, "ERROR", errTag, maxInt(1, sp.SpanCount))
			}
		}
		for _, tf := range snap.FailedTests {
			tfTrim := strings.TrimSpace(tf)
			if tfTrim != "" {
				add("test:"+tfTrim, "test_failure", "test-runner", "ERROR", "failed test: "+tfTrim, 1)
			}
		}
		if len(snap.ErrorClusters) == 0 && len(snap.FailedTests) == 0 && strings.TrimSpace(snap.RawError) != "" {
			add(snap.RawError, "job_error", "harness", "ERROR", snap.RawError, 1)
		}
		return m
	}

	baseMap := buildClusterMap(baseline)
	currMap := buildClusterMap(current)

	allKeys := make([]string, 0, len(baseMap)+len(currMap))
	seenKeys := make(map[string]bool, len(baseMap)+len(currMap))
	for k := range baseMap {
		seenKeys[k] = true
		allKeys = append(allKeys, k)
	}
	for k := range currMap {
		if !seenKeys[k] {
			seenKeys[k] = true
			allKeys = append(allKeys, k)
		}
	}
	sort.Strings(allKeys)

	for _, k := range allKeys {
		bItem, inBase := baseMap[k]
		cItem, inCurr := currMap[k]
		switch {
		case inBase && !inCurr:
			resolved = append(resolved, ErrorDiffEntry{
				Signature:     bItem.signature,
				Bucket:        BucketResolvedErrors,
				Category:      bItem.category,
				Service:       bItem.service,
				Level:         bItem.level,
				BaselineCount: bItem.count,
				CurrentCount:  0,
				DeltaCount:    -bItem.count,
				SampleMessage: bItem.sample,
			})
		case !inBase && inCurr:
			regressions = append(regressions, ErrorDiffEntry{
				Signature:     cItem.signature,
				Bucket:        BucketNewRegressions,
				Category:      cItem.category,
				Service:       cItem.service,
				Level:         cItem.level,
				BaselineCount: 0,
				CurrentCount:  cItem.count,
				DeltaCount:    cItem.count,
				SampleMessage: cItem.sample,
			})
		case inBase && inCurr:
			sample := cItem.sample
			if sample == "" {
				sample = bItem.sample
			}
			persistent = append(persistent, ErrorDiffEntry{
				Signature:     cItem.signature,
				Bucket:        BucketPersistentErrors,
				Category:      cItem.category,
				Service:       cItem.service,
				Level:         cItem.level,
				BaselineCount: bItem.count,
				CurrentCount:  cItem.count,
				DeltaCount:    cItem.count - bItem.count,
				SampleMessage: sample,
			})
		}
	}

	// Compare test failure counts even when individual test names are not enumerated
	baseFailures := baseline.TestFailureCount
	if len(baseline.FailedTests) > baseFailures {
		baseFailures = len(baseline.FailedTests)
	}
	currFailures := current.TestFailureCount
	if len(current.FailedTests) > currFailures {
		currFailures = len(current.FailedTests)
	}

	baseTestSet := make(map[string]bool, len(baseline.FailedTests))
	for _, t := range baseline.FailedTests {
		if trimmed := strings.TrimSpace(t); trimmed != "" {
			baseTestSet[trimmed] = true
		}
	}
	currTestSet := make(map[string]bool, len(current.FailedTests))
	for _, t := range current.FailedTests {
		if trimmed := strings.TrimSpace(t); trimmed != "" {
			currTestSet[trimmed] = true
		}
	}

	var resolvedTests, newFailedTests, persistentTests []string
	for t := range baseTestSet {
		if currTestSet[t] {
			persistentTests = append(persistentTests, t)
		} else {
			resolvedTests = append(resolvedTests, t)
		}
	}
	for t := range currTestSet {
		if !baseTestSet[t] {
			newFailedTests = append(newFailedTests, t)
		}
	}
	sort.Strings(resolvedTests)
	sort.Strings(newFailedTests)
	sort.Strings(persistentTests)

	// If aggregate test failure count changed without named tests, synthesize a test_failure_count diff entry
	if len(baseline.FailedTests) == 0 && len(current.FailedTests) == 0 && (baseFailures > 0 || currFailures > 0) {
		switch {
		case baseFailures > 0 && currFailures == 0:
			resolved = append(resolved, ErrorDiffEntry{
				Signature:     "test_failure_count",
				Bucket:        BucketResolvedErrors,
				Category:      "test_failure",
				Service:       "test-runner",
				Level:         "ERROR",
				BaselineCount: baseFailures,
				CurrentCount:  0,
				DeltaCount:    -baseFailures,
				SampleMessage: fmt.Sprintf("all %d baseline test failure(s) resolved", baseFailures),
			})
		case baseFailures == 0 && currFailures > 0:
			regressions = append(regressions, ErrorDiffEntry{
				Signature:     "test_failure_count",
				Bucket:        BucketNewRegressions,
				Category:      "test_failure",
				Service:       "test-runner",
				Level:         "ERROR",
				BaselineCount: 0,
				CurrentCount:  currFailures,
				DeltaCount:    currFailures,
				SampleMessage: fmt.Sprintf("%d new test failure(s) introduced", currFailures),
			})
		case currFailures > baseFailures:
			regressions = append(regressions, ErrorDiffEntry{
				Signature:     "test_failure_count_increase",
				Bucket:        BucketNewRegressions,
				Category:      "test_failure",
				Service:       "test-runner",
				Level:         "ERROR",
				BaselineCount: baseFailures,
				CurrentCount:  currFailures,
				DeltaCount:    currFailures - baseFailures,
				SampleMessage: fmt.Sprintf("test failures increased from %d to %d", baseFailures, currFailures),
			})
		case baseFailures > 0 && currFailures > 0:
			persistent = append(persistent, ErrorDiffEntry{
				Signature:     "test_failure_count",
				Bucket:        BucketPersistentErrors,
				Category:      "test_failure",
				Service:       "test-runner",
				Level:         "ERROR",
				BaselineCount: baseFailures,
				CurrentCount:  currFailures,
				DeltaCount:    currFailures - baseFailures,
				SampleMessage: fmt.Sprintf("test failures: baseline=%d current=%d", baseFailures, currFailures),
			})
		}
	}

	// Span latency comparison
	type spanAgg struct {
		spanName    string
		serviceName string
		maxMs       float64
		hasError    bool
		errorTag    string
	}
	buildSpanMap := func(items []SpanLatencyItem) map[string]*spanAgg {
		m := make(map[string]*spanAgg, len(items))
		for _, sp := range items {
			name := strings.TrimSpace(sp.SpanName)
			if name == "" {
				continue
			}
			svc := strings.TrimSpace(sp.ServiceName)
			key := strings.ToLower(svc) + "|" + strings.ToLower(name)
			dur := sp.DurationMs
			if sp.P95DurationMs > dur {
				dur = sp.P95DurationMs
			}
			if existing, ok := m[key]; ok {
				if dur > existing.maxMs {
					existing.maxMs = dur
				}
				if sp.HasError {
					existing.hasError = true
				}
				if existing.errorTag == "" && sp.ErrorTag != "" {
					existing.errorTag = sp.ErrorTag
				}
				continue
			}
			m[key] = &spanAgg{
				spanName:    name,
				serviceName: svc,
				maxMs:       dur,
				hasError:    sp.HasError,
				errorTag:    sp.ErrorTag,
			}
		}
		return m
	}

	baseSpans := buildSpanMap(baseline.SpanLatencies)
	currSpans := buildSpanMap(current.SpanLatencies)
	spanKeys := make([]string, 0, len(baseSpans)+len(currSpans))
	seenSpanKeys := make(map[string]bool)
	for k := range baseSpans {
		seenSpanKeys[k] = true
		spanKeys = append(spanKeys, k)
	}
	for k := range currSpans {
		if !seenSpanKeys[k] {
			seenSpanKeys[k] = true
			spanKeys = append(spanKeys, k)
		}
	}
	sort.Strings(spanKeys)

	anySpanLatencyRegressed := false
	for _, k := range spanKeys {
		bSp, inB := baseSpans[k]
		cSp, inC := currSpans[k]
		var spanName, svcName, errTag string
		var baseMs, currMs float64
		var baseErr, currErr bool
		if inB {
			spanName = bSp.spanName
			svcName = bSp.serviceName
			baseMs = bSp.maxMs
			baseErr = bSp.hasError
			errTag = bSp.errorTag
		}
		if inC {
			spanName = cSp.spanName
			if cSp.serviceName != "" {
				svcName = cSp.serviceName
			}
			currMs = cSp.maxMs
			currErr = cSp.hasError
			if cSp.errorTag != "" {
				errTag = cSp.errorTag
			}
		}
		deltaMs := round2(currMs - baseMs)
		var deltaPct float64
		if baseMs > 0 {
			deltaPct = round2(((currMs - baseMs) / baseMs) * 100.0)
		} else if currMs > 0 && inB {
			deltaPct = 100.0
		}
		regressed := inB && inC && deltaMs >= minDeltaMs && deltaPct >= thresholdPct
		if !baseErr && currErr {
			regressed = true
		}
		if regressed {
			anySpanLatencyRegressed = true
		}
		latencyDeltas = append(latencyDeltas, LatencyDeltaEntry{
			SpanName:      spanName,
			ServiceName:   svcName,
			MetricType:    "span_latency",
			BaselineMs:    round2(baseMs),
			CurrentMs:     round2(currMs),
			DeltaMs:       deltaMs,
			DeltaPct:      deltaPct,
			BaselineError: baseErr,
			CurrentError:  currErr,
			ErrorTag:      errTag,
			Regressed:     regressed,
		})
	}

	if baseline.DurationMs > 0 || current.DurationMs > 0 {
		deltaMs := round2(current.DurationMs - baseline.DurationMs)
		var deltaPct float64
		if baseline.DurationMs > 0 {
			deltaPct = round2(((current.DurationMs - baseline.DurationMs) / baseline.DurationMs) * 100.0)
		}
		regressed := baseline.DurationMs > 0 && current.DurationMs > 0 && deltaMs >= minDeltaMs && deltaPct >= thresholdPct
		latencyDeltas = append(latencyDeltas, LatencyDeltaEntry{
			SpanName:   "job.total_duration",
			MetricType: "job_duration",
			BaselineMs: round2(baseline.DurationMs),
			CurrentMs:  round2(current.DurationMs),
			DeltaMs:    deltaMs,
			DeltaPct:   deltaPct,
			Regressed:  regressed,
		})
	}

	verdict := VerdictCleanRun
	remediationVerified := false
	switch {
	case len(regressions) > 0:
		verdict = VerdictRegressionIntroduced
	case len(resolved) > 0 && len(persistent) == 0 && currFailures == 0:
		verdict = VerdictRemediationVerified
		remediationVerified = true
	case len(resolved) > 0 && len(regressions) == 0:
		verdict = VerdictRemediationVerified
		remediationVerified = true
	case len(persistent) > 0 || currFailures > 0:
		verdict = VerdictPersistentFailure
	default:
		verdict = VerdictCleanRun
		remediationVerified = true
	}

	storyID := strings.TrimSpace(current.StoryID)
	if storyID == "" {
		storyID = strings.TrimSpace(baseline.StoryID)
	}
	baseRunID := strings.TrimSpace(baseline.RunID)
	if baseRunID == "" {
		baseRunID = strings.TrimSpace(baseline.JobID)
	}
	if baseRunID == "" {
		baseRunID = "baseline"
	}
	currRunID := strings.TrimSpace(current.RunID)
	if currRunID == "" {
		currRunID = strings.TrimSpace(current.JobID)
	}
	if currRunID == "" {
		currRunID = "current"
	}

	mode := "job_to_job"
	if baseline.AttemptNumber > 0 && current.AttemptNumber > 0 && baseline.JobID != "" && baseline.JobID == current.JobID {
		mode = "attempt_to_attempt"
	} else if baseline.WindowLabel != "" || current.WindowLabel != "" {
		mode = "pre_post_window"
	}

	summary := fmt.Sprintf(
		"RunComparison [%s -> %s] verdict=%s: RESOLVED_ERRORS=%d, NEW_REGRESSIONS=%d, PERSISTENT_ERRORS=%d, LATENCY_DELTAS=%d (regressed_spans=%t), test_failures=%d->%d",
		baseRunID, currRunID, verdict,
		len(resolved), len(regressions), len(persistent), len(latencyDeltas),
		anySpanLatencyRegressed, baseFailures, currFailures,
	)

	diff := &RunComparisonDiff{
		BaselineRunID:       baseRunID,
		CurrentRunID:        currRunID,
		StoryID:             storyID,
		ComparisonMode:      mode,
		Verdict:             verdict,
		RemediationVerified: remediationVerified,
		ResolvedErrors:      resolved,
		NewRegressions:      regressions,
		PersistentErrors:    persistent,
		LatencyDeltas:       latencyDeltas,
		Buckets: RunComparisonBuckets{
			ResolvedErrors:   resolved,
			NewRegressions:   regressions,
			PersistentErrors: persistent,
			LatencyDeltas:    latencyDeltas,
		},
		TestFailureDelta: TestFailureDiffSummary{
			BaselineFailures: baseFailures,
			CurrentFailures:  currFailures,
			Delta:            currFailures - baseFailures,
			ResolvedTests:    resolvedTests,
			NewFailedTests:   newFailedTests,
			PersistentTests:  persistentTests,
		},
		EfficiencyDelta: EfficiencyDeltaSummary{
			BaselineTurns:      baseline.TurnCount,
			CurrentTurns:       current.TurnCount,
			DeltaTurns:         current.TurnCount - baseline.TurnCount,
			BaselineTokens:     baseline.TokenCount,
			CurrentTokens:      current.TokenCount,
			DeltaTokens:        current.TokenCount - baseline.TokenCount,
			BaselineDurationMs: round2(baseline.DurationMs),
			CurrentDurationMs:  round2(current.DurationMs),
			DeltaDurationMs:    round2(current.DurationMs - baseline.DurationMs),
		},
		Summary:    summary,
		ComparedAt: time.Now().UTC().Format(time.RFC3339),
	}
	diff.Phase4ReviewContext = FormatPhase4ReviewContext(diff)
	return diff
}

// FormatPhase4ReviewContext renders a deterministic Markdown summary for injection into ASE Phase 4 review agent context.
func FormatPhase4ReviewContext(diff *RunComparisonDiff) string {
	if diff == nil {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("### Run-to-Run Telemetry & Behavioral Diff (RunComparisonService)\n\n")
	fmt.Fprintf(&sb, "- **Baseline Run**: `%s` -> **Target Run**: `%s` (Mode: `%s`)\n", diff.BaselineRunID, diff.CurrentRunID, diff.ComparisonMode)
	fmt.Fprintf(&sb, "- **Remediation Verdict**: `%s` (Deterministic Proof: `remediation_verified=%t`)\n", diff.Verdict, diff.RemediationVerified)
	fmt.Fprintf(&sb, "- **Test Failure Delta**: `%d -> %d` (delta `%+d`)\n",
		diff.TestFailureDelta.BaselineFailures, diff.TestFailureDelta.CurrentFailures, diff.TestFailureDelta.Delta)
	fmt.Fprintf(&sb, "- **Efficiency Delta**: turns `%d -> %d` (`%+d`), tokens `%d -> %d` (`%+d`), duration `%.1fms -> %.1fms` (`%+.1fms`)\n",
		diff.EfficiencyDelta.BaselineTurns, diff.EfficiencyDelta.CurrentTurns, diff.EfficiencyDelta.DeltaTurns,
		diff.EfficiencyDelta.BaselineTokens, diff.EfficiencyDelta.CurrentTokens, diff.EfficiencyDelta.DeltaTokens,
		diff.EfficiencyDelta.BaselineDurationMs, diff.EfficiencyDelta.CurrentDurationMs, diff.EfficiencyDelta.DeltaDurationMs)
	sb.WriteString("\n#### Diff Buckets\n")

	// 1. RESOLVED_ERRORS
	fmt.Fprintf(&sb, "- **`%s`** (%d):\n", BucketResolvedErrors, len(diff.ResolvedErrors))
	if len(diff.ResolvedErrors) == 0 {
		sb.WriteString("  - None\n")
	} else {
		for _, e := range diff.ResolvedErrors {
			fmt.Fprintf(&sb, "  - `[%s]` `%s` (baseline=%d -> current=0)\n", e.Category, e.Signature, e.BaselineCount)
		}
	}

	// 2. NEW_REGRESSIONS
	fmt.Fprintf(&sb, "- **`%s`** (%d):\n", BucketNewRegressions, len(diff.NewRegressions))
	if len(diff.NewRegressions) == 0 {
		sb.WriteString("  - None (0 new error signatures introduced)\n")
	} else {
		for _, e := range diff.NewRegressions {
			fmt.Fprintf(&sb, "  - `[%s]` `%s` (baseline=0 -> current=%d)\n", e.Category, e.Signature, e.CurrentCount)
		}
	}

	// 3. PERSISTENT_ERRORS
	fmt.Fprintf(&sb, "- **`%s`** (%d):\n", BucketPersistentErrors, len(diff.PersistentErrors))
	if len(diff.PersistentErrors) == 0 {
		sb.WriteString("  - None\n")
	} else {
		for _, e := range diff.PersistentErrors {
			fmt.Fprintf(&sb, "  - `[%s]` `%s` (baseline=%d -> current=%d, delta=%+d)\n", e.Category, e.Signature, e.BaselineCount, e.CurrentCount, e.DeltaCount)
		}
	}

	// 4. LATENCY_DELTAS
	fmt.Fprintf(&sb, "- **`%s`** (%d):\n", BucketLatencyDeltas, len(diff.LatencyDeltas))
	if len(diff.LatencyDeltas) == 0 {
		sb.WriteString("  - None\n")
	} else {
		for _, l := range diff.LatencyDeltas {
			regTag := "OK"
			if l.Regressed {
				regTag = "REGRESSED"
			}
			fmt.Fprintf(&sb, "  - `%s` (%s): `%.1fms -> %.1fms` (`%+.1fms` / `%+.1f%%`) [%s]\n",
				l.SpanName, l.MetricType, l.BaselineMs, l.CurrentMs, l.DeltaMs, l.DeltaPct, regTag)
		}
	}
	return strings.TrimSpace(sb.String())
}

// ExtractJobSnapshot builds a RunTelemetrySnapshot from a dispatch.Job and optional log lines.
func ExtractJobSnapshot(job *Job, logLines []string) RunTelemetrySnapshot {
	if job == nil {
		return RunTelemetrySnapshot{}
	}
	runID := job.ID
	if job.Attempts > 0 {
		runID = fmt.Sprintf("%s#attempt-%d", job.ID, job.Attempts)
	}
	var durationMs float64
	if job.ElapsedSeconds > 0 {
		durationMs = float64(job.ElapsedSeconds) * 1000.0
	} else if job.ClaimedAt != nil && job.CompletedAt != nil {
		durationMs = float64(job.CompletedAt.Sub(*job.ClaimedAt).Milliseconds())
	} else if !job.CreatedAt.IsZero() && job.CompletedAt != nil {
		durationMs = float64(job.CompletedAt.Sub(job.CreatedAt).Milliseconds())
	}

	snap := RunTelemetrySnapshot{
		RunID:         runID,
		JobID:         job.ID,
		StoryID:       job.StoryID,
		AttemptNumber: job.Attempts,
		Status:        job.Status,
		DurationMs:    durationMs,
		TurnCount:     job.FinalTurnCount,
		RawError:      strings.TrimSpace(job.Error),
	}
	if job.ClaimedAt != nil {
		snap.StartedAt = job.ClaimedAt.UTC().Format(time.RFC3339)
	} else if !job.CreatedAt.IsZero() {
		snap.StartedAt = job.CreatedAt.UTC().Format(time.RFC3339)
	}
	if job.CompletedAt != nil {
		snap.CompletedAt = job.CompletedAt.UTC().Format(time.RFC3339)
	}

	// Parse structured telemetry from Job.Result if present
	if len(job.Result) > 0 {
		var resPayload struct {
			TestFailureCount  int                `json:"test_failure_count"`
			TestPassCount     int                `json:"test_pass_count"`
			FailedTests       []string           `json:"failed_tests"`
			TokenCount        int                `json:"token_count"`
			TurnCount         int                `json:"turn_count"`
			DurationMs        float64            `json:"duration_ms"`
			CommitSHA         string             `json:"commit_sha"`
			ErrorClusters     []ErrorClusterItem `json:"error_clusters"`
			WarningSignatures []string           `json:"warning_signatures"`
			SpanLatencies     []SpanLatencyItem  `json:"span_latencies"`
		}
		if err := json.Unmarshal(job.Result, &resPayload); err == nil {
			if resPayload.TestFailureCount > 0 {
				snap.TestFailureCount = resPayload.TestFailureCount
			}
			if resPayload.TestPassCount > 0 {
				snap.TestPassCount = resPayload.TestPassCount
			}
			if len(resPayload.FailedTests) > 0 {
				snap.FailedTests = append(snap.FailedTests, resPayload.FailedTests...)
			}
			if resPayload.TokenCount > 0 {
				snap.TokenCount = resPayload.TokenCount
			}
			if resPayload.TurnCount > 0 && snap.TurnCount == 0 {
				snap.TurnCount = resPayload.TurnCount
			}
			if resPayload.DurationMs > 0 && snap.DurationMs == 0 {
				snap.DurationMs = resPayload.DurationMs
			}
			if resPayload.CommitSHA != "" {
				snap.CommitSHA = resPayload.CommitSHA
			}
			if len(resPayload.ErrorClusters) > 0 {
				snap.ErrorClusters = append(snap.ErrorClusters, resPayload.ErrorClusters...)
			}
			if len(resPayload.WarningSignatures) > 0 {
				snap.WarningSignatures = append(snap.WarningSignatures, resPayload.WarningSignatures...)
			}
			if len(resPayload.SpanLatencies) > 0 {
				snap.SpanLatencies = append(snap.SpanLatencies, resPayload.SpanLatencies...)
			}
		}
	}

	if snap.RawError != "" {
		for _, m := range goTestFail.FindAllStringSubmatch(snap.RawError, -1) {
			if len(m) > 1 {
				snap.FailedTests = appendUniqueString(snap.FailedTests, m[1])
			}
		}
		if len(snap.ErrorClusters) == 0 {
			snap.ErrorClusters = append(snap.ErrorClusters, ErrorClusterItem{
				Signature:     NormalizeErrorSignature(snap.RawError),
				Level:         "ERROR",
				Service:       "harness-job",
				Category:      "error_cluster",
				Count:         1,
				SampleMessage: snap.RawError,
			})
		}
	}

	// Parse error/warning clusters and test failures from log lines if provided
	clusterCounts := make(map[string]*ErrorClusterItem)
	for _, line := range logLines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		for _, m := range goTestFail.FindAllStringSubmatch(trimmed, -1) {
			if len(m) > 1 {
				snap.FailedTests = appendUniqueString(snap.FailedTests, m[1])
			}
		}
		upper := strings.ToUpper(trimmed)
		switch {
		case strings.Contains(upper, "ERROR") || strings.Contains(upper, "FATAL") || strings.Contains(upper, "PANIC:"):
			sig := NormalizeErrorSignature(trimmed)
			if sig != "" {
				if existing, ok := clusterCounts[sig]; ok {
					existing.Count++
				} else {
					clusterCounts[sig] = &ErrorClusterItem{
						Signature:     sig,
						Level:         "ERROR",
						Service:       "job-log",
						Category:      "error_cluster",
						Count:         1,
						SampleMessage: trimmed,
					}
				}
			}
		case strings.Contains(upper, "WARN"):
			sig := NormalizeErrorSignature(trimmed)
			if sig != "" {
				snap.WarningSignatures = appendUniqueString(snap.WarningSignatures, sig)
			}
		}
	}
	if len(clusterCounts) > 0 {
		keys := make([]string, 0, len(clusterCounts))
		for k := range clusterCounts {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			snap.ErrorClusters = append(snap.ErrorClusters, *clusterCounts[k])
		}
	}
	if len(snap.FailedTests) > snap.TestFailureCount {
		snap.TestFailureCount = len(snap.FailedTests)
	}
	return snap
}

// ExtractBaselineSnapshotFromAttemptHistory builds a baseline RunTelemetrySnapshot from the prior AttemptFailure on a retried job.
func ExtractBaselineSnapshotFromAttemptHistory(job *Job) (RunTelemetrySnapshot, bool) {
	if job == nil || len(job.AttemptHistory) == 0 {
		return RunTelemetrySnapshot{}, false
	}
	prev := job.AttemptHistory[len(job.AttemptHistory)-1]
	attemptNum := prev.AttemptNumber
	if attemptNum <= 0 {
		attemptNum = len(job.AttemptHistory)
	}
	runID := fmt.Sprintf("%s#attempt-%d", job.ID, attemptNum)
	var durationMs float64
	if prev.StartedAt != "" && prev.EndedAt != "" {
		if tStart, err1 := time.Parse(time.RFC3339, prev.StartedAt); err1 == nil {
			if tEnd, err2 := time.Parse(time.RFC3339, prev.EndedAt); err2 == nil && tEnd.After(tStart) {
				durationMs = float64(tEnd.Sub(tStart).Milliseconds())
			}
		}
	}
	rawErr := strings.TrimSpace(prev.StderrTail)
	if rawErr == "" {
		rawErr = strings.TrimSpace(prev.Reason)
	}
	if rawErr == "" {
		rawErr = strings.TrimSpace(prev.OutcomeSummary)
	}
	snap := RunTelemetrySnapshot{
		RunID:         runID,
		JobID:         job.ID,
		StoryID:       job.StoryID,
		AttemptNumber: attemptNum,
		Status:        JobStatusFailed,
		WindowLabel:   fmt.Sprintf("attempt-%d-baseline", attemptNum),
		StartedAt:     prev.StartedAt,
		CompletedAt:   prev.EndedAt,
		DurationMs:    durationMs,
		RawError:      rawErr,
	}
	for _, m := range goTestFail.FindAllStringSubmatch(rawErr, -1) {
		if len(m) > 1 {
			snap.FailedTests = appendUniqueString(snap.FailedTests, m[1])
		}
	}
	if len(snap.FailedTests) > 0 {
		snap.TestFailureCount = len(snap.FailedTests)
	}
	if rawErr != "" {
		snap.ErrorClusters = []ErrorClusterItem{
			{
				Signature:     NormalizeErrorSignature(rawErr),
				Level:         "ERROR",
				Service:       "harness-job",
				Category:      "error_cluster",
				Count:         1,
				SampleMessage: rawErr,
			},
		}
	}
	return snap, true
}

// CompareJobWithBaseline automatically computes a RunComparisonDiff for a job if a baseline job or prior retry attempt exists.
func CompareJobWithBaseline(target *Job, baselineJob *Job, targetLogs, baselineLogs []string) *RunComparisonDiff {
	if target == nil {
		return nil
	}
	svc := NewRunComparisonService()
	currSnap := ExtractJobSnapshot(target, targetLogs)
	if baselineJob != nil && baselineJob.ID != "" && baselineJob.ID != target.ID {
		baseSnap := ExtractJobSnapshot(baselineJob, baselineLogs)
		return svc.CompareRuns(baseSnap, currSnap)
	}
	if baseSnap, ok := ExtractBaselineSnapshotFromAttemptHistory(target); ok {
		return svc.CompareRuns(baseSnap, currSnap)
	}
	return nil
}

func appendUniqueString(slice []string, val string) []string {
	for _, item := range slice {
		if item == val {
			return slice
		}
	}
	return append(slice, val)
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
