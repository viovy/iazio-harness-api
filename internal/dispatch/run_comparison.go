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

const (
	levelError            = "ERROR"
	levelWarn             = "WARN"
	catErrorCluster       = "error_cluster"
	catWarningSignature   = "warning_signature"
	catSpanErrorTag       = "span_error_tag"
	catTestFailure        = "test_failure"
	catJobError           = "job_error"
	svcTestRunner         = "test-runner"
	svcHarnessJob         = "harness-job"
	sigTestFailureCount   = "test_failure_count"
	metricSpanLatency     = "span_latency"
	metricJobDuration     = "job_duration"
	spanTotalDuration     = "job.total_duration"
	noneBulletLine        = "  - None\n"
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
	Category      string `json:"category,omitempty"`
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
	Bucket        string `json:"bucket"`
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
	MetricType    string  `json:"metric_type,omitempty"`
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
	hexTokenRe = regexp.MustCompile(`\b[0-9a-fA-F]{8,64}\b`)
	numTokenRe = regexp.MustCompile(`\b\d{4,}\b`)
	goTestFail = regexp.MustCompile(`(?m)^--- FAIL:\s+([A-Za-z0-9_/-]+)`)
	wsCollapse = regexp.MustCompile(`\s+`)
)

// NormalizeErrorSignature canonicalizes a raw error message or cluster signature for deterministic comparison.
func NormalizeErrorSignature(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	if strings.Contains(s, "\n") && strings.Contains(s, "ALLOCATED_STORY_ID:") {
		lines := strings.Split(s, "\n")
		for i := len(lines) - 1; i >= 0; i-- {
			cand := strings.TrimSpace(lines[i])
			if cand != "" && !strings.Contains(cand, "ALLOCATED_STORY_ID:") {
				s = cand
				break
			}
		}
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

type aggCluster struct {
	signature string
	category  string
	service   string
	level     string
	count     int
	sample    string
}

func addClusterItem(m map[string]*aggCluster, sig, category, service, level, sample string, count int) {
	norm := NormalizeErrorSignature(sig)
	if norm == "" {
		return
	}
	if count <= 0 {
		count = 1
	}
	if category == "" {
		category = catErrorCluster
	}
	if level == "" {
		level = levelError
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

func resolveClusterCategory(c ErrorClusterItem) string {
	if c.Category != "" {
		return c.Category
	}
	if strings.EqualFold(c.Level, levelWarn) || strings.EqualFold(c.Level, "WARNING") {
		return catWarningSignature
	}
	return catErrorCluster
}

func resolveSpanErrorTag(sp SpanLatencyItem) (string, bool) {
	errTag := strings.TrimSpace(sp.ErrorTag)
	status := strings.TrimSpace(sp.StatusCode)
	if !sp.HasError && errTag == "" && !strings.HasPrefix(status, "5") {
		return "", false
	}
	if errTag != "" {
		return errTag, true
	}
	if status != "" {
		return "http.status_code=" + status, true
	}
	return "error=true", true
}

func buildSnapshotClusterMap(snap RunTelemetrySnapshot) map[string]*aggCluster {
	m := make(map[string]*aggCluster)
	for _, c := range snap.ErrorClusters {
		addClusterItem(m, c.Signature, resolveClusterCategory(c), c.Service, c.Level, c.SampleMessage, c.Count)
	}
	for _, w := range snap.WarningSignatures {
		addClusterItem(m, w, catWarningSignature, "", levelWarn, w, 1)
	}
	for _, sp := range snap.SpanLatencies {
		if errTag, hasErr := resolveSpanErrorTag(sp); hasErr {
			sig := fmt.Sprintf("span:%s (%s)", sp.SpanName, errTag)
			addClusterItem(m, sig, catSpanErrorTag, sp.ServiceName, levelError, errTag, maxInt(1, sp.SpanCount))
		}
	}
	for _, tf := range snap.FailedTests {
		if tfTrim := strings.TrimSpace(tf); tfTrim != "" {
			addClusterItem(m, "test:"+tfTrim, catTestFailure, svcTestRunner, levelError, "failed test: "+tfTrim, 1)
		}
	}
	if len(snap.ErrorClusters) == 0 && len(snap.FailedTests) == 0 && strings.TrimSpace(snap.RawError) != "" {
		addClusterItem(m, snap.RawError, catJobError, "harness", levelError, snap.RawError, 1)
	}
	return m
}

func sortedUnionKeys[V any](a, b map[string]V) []string {
	keys := make([]string, 0, len(a)+len(b))
	seen := make(map[string]bool, len(a)+len(b))
	for k := range a {
		seen[k] = true
		keys = append(keys, k)
	}
	for k := range b {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

func classifyClusterDiffs(baseMap, currMap map[string]*aggCluster) (resolved, regressions, persistent []ErrorDiffEntry) {
	resolved = make([]ErrorDiffEntry, 0)
	regressions = make([]ErrorDiffEntry, 0)
	persistent = make([]ErrorDiffEntry, 0)

	for _, k := range sortedUnionKeys(baseMap, currMap) {
		bItem, inBase := baseMap[k]
		cItem, inCurr := currMap[k]
		if inBase && !inCurr {
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
			continue
		}
		if !inBase && inCurr {
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
			continue
		}
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
	return resolved, regressions, persistent
}

func computeTestFailureSummary(baseline, current RunTelemetrySnapshot) TestFailureDiffSummary {
	baseFailures := maxInt(baseline.TestFailureCount, len(baseline.FailedTests))
	currFailures := maxInt(current.TestFailureCount, len(current.FailedTests))

	baseSet := toTrimmedSet(baseline.FailedTests)
	currSet := toTrimmedSet(current.FailedTests)

	var resolvedTests, newFailedTests, persistentTests []string
	for t := range baseSet {
		if currSet[t] {
			persistentTests = append(persistentTests, t)
		} else {
			resolvedTests = append(resolvedTests, t)
		}
	}
	for t := range currSet {
		if !baseSet[t] {
			newFailedTests = append(newFailedTests, t)
		}
	}
	sort.Strings(resolvedTests)
	sort.Strings(newFailedTests)
	sort.Strings(persistentTests)

	return TestFailureDiffSummary{
		BaselineFailures: baseFailures,
		CurrentFailures:  currFailures,
		Delta:            currFailures - baseFailures,
		ResolvedTests:    resolvedTests,
		NewFailedTests:   newFailedTests,
		PersistentTests:  persistentTests,
	}
}

func toTrimmedSet(items []string) map[string]bool {
	set := make(map[string]bool, len(items))
	for _, it := range items {
		if trimmed := strings.TrimSpace(it); trimmed != "" {
			set[trimmed] = true
		}
	}
	return set
}

func appendAggregateTestCountDiff(
	baseline, current RunTelemetrySnapshot,
	testSummary TestFailureDiffSummary,
	resolved, regressions, persistent []ErrorDiffEntry,
) ([]ErrorDiffEntry, []ErrorDiffEntry, []ErrorDiffEntry) {
	if len(baseline.FailedTests) > 0 || len(current.FailedTests) > 0 {
		return resolved, regressions, persistent
	}
	baseFailures := testSummary.BaselineFailures
	currFailures := testSummary.CurrentFailures
	if baseFailures == 0 && currFailures == 0 {
		return resolved, regressions, persistent
	}
	if baseFailures > 0 && currFailures == 0 {
		resolved = append(resolved, ErrorDiffEntry{
			Signature:     sigTestFailureCount,
			Bucket:        BucketResolvedErrors,
			Category:      catTestFailure,
			Service:       svcTestRunner,
			Level:         levelError,
			BaselineCount: baseFailures,
			CurrentCount:  0,
			DeltaCount:    -baseFailures,
			SampleMessage: fmt.Sprintf("all %d baseline test failure(s) resolved", baseFailures),
		})
		return resolved, regressions, persistent
	}
	if baseFailures == 0 && currFailures > 0 {
		regressions = append(regressions, ErrorDiffEntry{
			Signature:     sigTestFailureCount,
			Bucket:        BucketNewRegressions,
			Category:      catTestFailure,
			Service:       svcTestRunner,
			Level:         levelError,
			BaselineCount: 0,
			CurrentCount:  currFailures,
			DeltaCount:    currFailures,
			SampleMessage: fmt.Sprintf("%d new test failure(s) introduced", currFailures),
		})
		return resolved, regressions, persistent
	}
	if currFailures > baseFailures {
		regressions = append(regressions, ErrorDiffEntry{
			Signature:     "test_failure_count_increase",
			Bucket:        BucketNewRegressions,
			Category:      catTestFailure,
			Service:       svcTestRunner,
			Level:         levelError,
			BaselineCount: baseFailures,
			CurrentCount:  currFailures,
			DeltaCount:    currFailures - baseFailures,
			SampleMessage: fmt.Sprintf("test failures increased from %d to %d", baseFailures, currFailures),
		})
		return resolved, regressions, persistent
	}
	persistent = append(persistent, ErrorDiffEntry{
		Signature:     sigTestFailureCount,
		Bucket:        BucketPersistentErrors,
		Category:      catTestFailure,
		Service:       svcTestRunner,
		Level:         levelError,
		BaselineCount: baseFailures,
		CurrentCount:  currFailures,
		DeltaCount:    currFailures - baseFailures,
		SampleMessage: fmt.Sprintf("test failures: baseline=%d current=%d", baseFailures, currFailures),
	})
	return resolved, regressions, persistent
}

type spanAgg struct {
	spanName    string
	serviceName string
	maxMs       float64
	hasError    bool
	errorTag    string
}

func buildSpanAggMap(items []SpanLatencyItem) map[string]*spanAgg {
	m := make(map[string]*spanAgg, len(items))
	for _, sp := range items {
		name := strings.TrimSpace(sp.SpanName)
		if name == "" {
			continue
		}
		svc := strings.TrimSpace(sp.ServiceName)
		key := strings.ToLower(svc) + "|" + strings.ToLower(name)
		dur := math.Max(sp.DurationMs, sp.P95DurationMs)
		if existing, ok := m[key]; ok {
			existing.maxMs = math.Max(existing.maxMs, dur)
			existing.hasError = existing.hasError || sp.HasError
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

func buildSingleSpanDelta(bSp *spanAgg, inB bool, cSp *spanAgg, inC bool, minDeltaMs, thresholdPct float64) LatencyDeltaEntry {
	var entry LatencyDeltaEntry
	entry.MetricType = metricSpanLatency
	if inB {
		entry.SpanName = bSp.spanName
		entry.ServiceName = bSp.serviceName
		entry.BaselineMs = round2(bSp.maxMs)
		entry.BaselineError = bSp.hasError
		entry.ErrorTag = bSp.errorTag
	}
	if inC {
		entry.SpanName = cSp.spanName
		if cSp.serviceName != "" {
			entry.ServiceName = cSp.serviceName
		}
		entry.CurrentMs = round2(cSp.maxMs)
		entry.CurrentError = cSp.hasError
		if cSp.errorTag != "" {
			entry.ErrorTag = cSp.errorTag
		}
	}
	entry.DeltaMs = round2(entry.CurrentMs - entry.BaselineMs)
	if entry.BaselineMs > 0 {
		entry.DeltaPct = round2(((entry.CurrentMs - entry.BaselineMs) / entry.BaselineMs) * 100.0)
	} else if entry.CurrentMs > 0 && inB {
		entry.DeltaPct = 100.0
	}
	entry.Regressed = (inB && inC && entry.DeltaMs >= minDeltaMs && entry.DeltaPct >= thresholdPct) || (!entry.BaselineError && entry.CurrentError)
	return entry
}

func computeLatencyDeltas(baseline, current RunTelemetrySnapshot, minDeltaMs, thresholdPct float64) ([]LatencyDeltaEntry, bool) {
	baseSpans := buildSpanAggMap(baseline.SpanLatencies)
	currSpans := buildSpanAggMap(current.SpanLatencies)
	latencyDeltas := make([]LatencyDeltaEntry, 0, len(baseSpans)+len(currSpans)+1)
	anySpanRegressed := false

	for _, k := range sortedUnionKeys(baseSpans, currSpans) {
		bSp, inB := baseSpans[k]
		cSp, inC := currSpans[k]
		delta := buildSingleSpanDelta(bSp, inB, cSp, inC, minDeltaMs, thresholdPct)
		if delta.Regressed {
			anySpanRegressed = true
		}
		latencyDeltas = append(latencyDeltas, delta)
	}

	if baseline.DurationMs > 0 || current.DurationMs > 0 {
		deltaMs := round2(current.DurationMs - baseline.DurationMs)
		var deltaPct float64
		if baseline.DurationMs > 0 {
			deltaPct = round2(((current.DurationMs - baseline.DurationMs) / baseline.DurationMs) * 100.0)
		}
		regressed := baseline.DurationMs > 0 && current.DurationMs > 0 && deltaMs >= minDeltaMs && deltaPct >= thresholdPct
		latencyDeltas = append(latencyDeltas, LatencyDeltaEntry{
			SpanName:   spanTotalDuration,
			MetricType: metricJobDuration,
			BaselineMs: round2(baseline.DurationMs),
			CurrentMs:  round2(current.DurationMs),
			DeltaMs:    deltaMs,
			DeltaPct:   deltaPct,
			Regressed:  regressed,
		})
	}
	return latencyDeltas, anySpanRegressed
}

func determineComparisonVerdict(resolvedCount, regressionCount, persistentCount, currFailures int) (string, bool) {
	if regressionCount > 0 {
		return VerdictRegressionIntroduced, false
	}
	if resolvedCount > 0 {
		return VerdictRemediationVerified, true
	}
	if persistentCount > 0 || currFailures > 0 {
		return VerdictPersistentFailure, false
	}
	return VerdictCleanRun, true
}

func resolveRunIdentifier(runID, jobID, fallback string) string {
	if s := strings.TrimSpace(runID); s != "" {
		return s
	}
	if s := strings.TrimSpace(jobID); s != "" {
		return s
	}
	return fallback
}

func resolveComparisonMode(baseline, current RunTelemetrySnapshot) string {
	if baseline.AttemptNumber > 0 && current.AttemptNumber > 0 && baseline.JobID != "" && baseline.JobID == current.JobID {
		return "attempt_to_attempt"
	}
	if baseline.WindowLabel != "" || current.WindowLabel != "" {
		return "pre_post_window"
	}
	return "job_to_job"
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

	baseMap := buildSnapshotClusterMap(baseline)
	currMap := buildSnapshotClusterMap(current)
	resolved, regressions, persistent := classifyClusterDiffs(baseMap, currMap)
	testSummary := computeTestFailureSummary(baseline, current)
	resolved, regressions, persistent = appendAggregateTestCountDiff(baseline, current, testSummary, resolved, regressions, persistent)
	latencyDeltas, anySpanRegressed := computeLatencyDeltas(baseline, current, minDeltaMs, thresholdPct)
	verdict, remediationVerified := determineComparisonVerdict(len(resolved), len(regressions), len(persistent), testSummary.CurrentFailures)

	storyID := firstNonEmptyTrimmed(current.StoryID, baseline.StoryID)
	baseRunID := resolveRunIdentifier(baseline.RunID, baseline.JobID, "baseline")
	currRunID := resolveRunIdentifier(current.RunID, current.JobID, "current")
	mode := resolveComparisonMode(baseline, current)

	summary := fmt.Sprintf(
		"RunComparison [%s -> %s] verdict=%s: RESOLVED_ERRORS=%d, NEW_REGRESSIONS=%d, PERSISTENT_ERRORS=%d, LATENCY_DELTAS=%d (regressed_spans=%t), test_failures=%d->%d",
		baseRunID, currRunID, verdict,
		len(resolved), len(regressions), len(persistent), len(latencyDeltas),
		anySpanRegressed, testSummary.BaselineFailures, testSummary.CurrentFailures,
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
		TestFailureDelta: testSummary,
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

func writeErrorBucketMarkdown(sb *strings.Builder, bucketName, emptyText string, entries []ErrorDiffEntry, formatEntry func(ErrorDiffEntry) string) {
	fmt.Fprintf(sb, "- **`%s`** (%d):\n", bucketName, len(entries))
	if len(entries) == 0 {
		sb.WriteString(emptyText)
		return
	}
	for _, e := range entries {
		sb.WriteString(formatEntry(e))
	}
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

	writeErrorBucketMarkdown(&sb, BucketResolvedErrors, noneBulletLine, diff.ResolvedErrors, func(e ErrorDiffEntry) string {
		return fmt.Sprintf("  - `[%s]` `%s` (baseline=%d -> current=0)\n", e.Category, e.Signature, e.BaselineCount)
	})
	writeErrorBucketMarkdown(&sb, BucketNewRegressions, "  - None (0 new error signatures introduced)\n", diff.NewRegressions, func(e ErrorDiffEntry) string {
		return fmt.Sprintf("  - `[%s]` `%s` (baseline=0 -> current=%d)\n", e.Category, e.Signature, e.CurrentCount)
	})
	writeErrorBucketMarkdown(&sb, BucketPersistentErrors, noneBulletLine, diff.PersistentErrors, func(e ErrorDiffEntry) string {
		return fmt.Sprintf("  - `[%s]` `%s` (baseline=%d -> current=%d, delta=%+d)\n", e.Category, e.Signature, e.BaselineCount, e.CurrentCount, e.DeltaCount)
	})

	fmt.Fprintf(&sb, "- **`%s`** (%d):\n", BucketLatencyDeltas, len(diff.LatencyDeltas))
	if len(diff.LatencyDeltas) == 0 {
		sb.WriteString(noneBulletLine)
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

type jobResultTelemetryPayload struct {
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

func computeJobDurationMs(job *Job) float64 {
	if job.ElapsedSeconds > 0 {
		return float64(job.ElapsedSeconds) * 1000.0
	}
	if job.ClaimedAt != nil && job.CompletedAt != nil {
		return float64(job.CompletedAt.Sub(*job.ClaimedAt).Milliseconds())
	}
	if !job.CreatedAt.IsZero() && job.CompletedAt != nil {
		return float64(job.CompletedAt.Sub(job.CreatedAt).Milliseconds())
	}
	return 0
}

func applyJobResultTelemetry(snap *RunTelemetrySnapshot, rawResult []byte) {
	if len(rawResult) == 0 {
		return
	}
	var res jobResultTelemetryPayload
	if err := json.Unmarshal(rawResult, &res); err != nil {
		return
	}
	snap.TestFailureCount = maxInt(snap.TestFailureCount, res.TestFailureCount)
	snap.TestPassCount = maxInt(snap.TestPassCount, res.TestPassCount)
	snap.FailedTests = append(snap.FailedTests, res.FailedTests...)
	snap.TokenCount = maxInt(snap.TokenCount, res.TokenCount)
	if res.TurnCount > 0 && snap.TurnCount == 0 {
		snap.TurnCount = res.TurnCount
	}
	if res.DurationMs > 0 && snap.DurationMs == 0 {
		snap.DurationMs = res.DurationMs
	}
	if res.CommitSHA != "" {
		snap.CommitSHA = res.CommitSHA
	}
	snap.ErrorClusters = append(snap.ErrorClusters, res.ErrorClusters...)
	snap.WarningSignatures = append(snap.WarningSignatures, res.WarningSignatures...)
	snap.SpanLatencies = append(snap.SpanLatencies, res.SpanLatencies...)
}

func extractFailedTestsFromText(existing []string, text string) []string {
	for _, m := range goTestFail.FindAllStringSubmatch(text, -1) {
		if len(m) > 1 {
			existing = appendUniqueString(existing, m[1])
		}
	}
	return existing
}

func parseLogLinesIntoSnapshot(snap *RunTelemetrySnapshot, logLines []string) {
	clusterCounts := make(map[string]*ErrorClusterItem)
	for _, line := range logLines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		snap.FailedTests = extractFailedTestsFromText(snap.FailedTests, trimmed)
		upper := strings.ToUpper(trimmed)
		if strings.Contains(upper, levelError) || strings.Contains(upper, "FATAL") || strings.Contains(upper, "PANIC:") {
			addLogErrorCluster(clusterCounts, trimmed)
		} else if strings.Contains(upper, levelWarn) {
			if sig := NormalizeErrorSignature(trimmed); sig != "" {
				snap.WarningSignatures = appendUniqueString(snap.WarningSignatures, sig)
			}
		}
	}
	keys := make([]string, 0, len(clusterCounts))
	for k := range clusterCounts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		snap.ErrorClusters = append(snap.ErrorClusters, *clusterCounts[k])
	}
}

func addLogErrorCluster(clusterCounts map[string]*ErrorClusterItem, trimmed string) {
	sig := NormalizeErrorSignature(trimmed)
	if sig == "" {
		return
	}
	if existing, ok := clusterCounts[sig]; ok {
		existing.Count++
		return
	}
	clusterCounts[sig] = &ErrorClusterItem{
		Signature:     sig,
		Level:         levelError,
		Service:       "job-log",
		Category:      catErrorCluster,
		Count:         1,
		SampleMessage: trimmed,
	}
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
	snap := RunTelemetrySnapshot{
		RunID:         runID,
		JobID:         job.ID,
		StoryID:       job.StoryID,
		AttemptNumber: job.Attempts,
		Status:        job.Status,
		DurationMs:    computeJobDurationMs(job),
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

	applyJobResultTelemetry(&snap, job.Result)
	if strings.EqualFold(strings.TrimSpace(job.Status), JobStatusCompleted) && snap.RawError == "" {
		snap.ErrorClusters = nil
		snap.FailedTests = nil
		snap.TestFailureCount = 0
		return snap
	}
	if snap.RawError != "" {
		snap.FailedTests = extractFailedTestsFromText(snap.FailedTests, snap.RawError)
		if len(snap.ErrorClusters) == 0 {
			snap.ErrorClusters = append(snap.ErrorClusters, ErrorClusterItem{
				Signature:     NormalizeErrorSignature(snap.RawError),
				Level:         levelError,
				Service:       svcHarnessJob,
				Category:      catErrorCluster,
				Count:         1,
				SampleMessage: snap.RawError,
			})
		}
	}
	parseLogLinesIntoSnapshot(&snap, logLines)
	snap.TestFailureCount = maxInt(snap.TestFailureCount, len(snap.FailedTests))
	return snap
}

func parseRFC3339DurationMs(startedAt, endedAt string) float64 {
	if startedAt == "" || endedAt == "" {
		return 0
	}
	tStart, err1 := time.Parse(time.RFC3339, startedAt)
	tEnd, err2 := time.Parse(time.RFC3339, endedAt)
	if err1 != nil || err2 != nil || !tEnd.After(tStart) {
		return 0
	}
	return float64(tEnd.Sub(tStart).Milliseconds())
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
	rawErr := firstNonEmptyTrimmed(prev.StderrTail, prev.Reason, prev.OutcomeSummary)
	snap := RunTelemetrySnapshot{
		RunID:         fmt.Sprintf("%s#attempt-%d", job.ID, attemptNum),
		JobID:         job.ID,
		StoryID:       job.StoryID,
		AttemptNumber: attemptNum,
		Status:        JobStatusFailed,
		WindowLabel:   fmt.Sprintf("attempt-%d-baseline", attemptNum),
		StartedAt:     prev.StartedAt,
		CompletedAt:   prev.EndedAt,
		DurationMs:    parseRFC3339DurationMs(prev.StartedAt, prev.EndedAt),
		RawError:      rawErr,
		FailedTests:   extractFailedTestsFromText(nil, rawErr),
	}
	// STORY-IAZIO-0202: StderrTail may hold a preserved pre-supersede Result JSON envelope.
	if strings.TrimSpace(prev.StderrTail) != "" {
		applyJobResultTelemetry(&snap, []byte(prev.StderrTail))
	}
	snap.TestFailureCount = maxInt(snap.TestFailureCount, len(snap.FailedTests))
	if rawErr != "" && len(snap.ErrorClusters) == 0 {
		snap.ErrorClusters = []ErrorClusterItem{
			{
				Signature:     NormalizeErrorSignature(rawErr),
				Level:         levelError,
				Service:       svcHarnessJob,
				Category:      catErrorCluster,
				Count:         1,
				SampleMessage: rawErr,
			},
		}
	}
	return snap, true
}

func snapshotLooksClean(snap RunTelemetrySnapshot) bool {
	return snap.TestFailureCount == 0 && len(snap.FailedTests) == 0 &&
		len(snap.ErrorClusters) == 0 && strings.TrimSpace(snap.RawError) == ""
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
		// After STORY-IAZIO-0202 supersede, parent Result may be the child's clean success —
		// fall back to preserved pre-supersede AttemptHistory on the baseline job.
		if snapshotLooksClean(baseSnap) {
			if histSnap, ok := ExtractBaselineSnapshotFromAttemptHistory(baselineJob); ok {
				baseSnap = histSnap
			}
		}
		return svc.CompareRuns(baseSnap, currSnap)
	}
	if baseSnap, ok := ExtractBaselineSnapshotFromAttemptHistory(target); ok {
		return svc.CompareRuns(baseSnap, currSnap)
	}
	return nil
}

func firstNonEmptyTrimmed(vals ...string) string {
	for _, v := range vals {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
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
