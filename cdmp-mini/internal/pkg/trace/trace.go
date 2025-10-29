package trace

import (
	"context"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	uuid "github.com/satori/go.uuid"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/code"
	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/pkg/metrics"
	"github.com/maxiaolu1981/cretem/cdmp-mini/pkg/log"
)

var json = jsoniter.Config{
	EscapeHTML:                    true,
	SortMapKeys:                   false,
	ValidateJsonRawMessage:        true,
	ObjectFieldMustBeSimpleString: true,
}.Froze()

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Phase represents the lifecycle phase of a trace payload.
type Phase string

const (
	// PhaseHTTP marks the HTTP request handling phase.
	PhaseHTTP Phase = "http"
	// PhaseAsync marks the asynchronous processing phase (e.g., Kafka consumer).
	PhaseAsync Phase = "async"
)

// Options describes the initialization parameters for a new trace session.
type Options struct {
	TraceID         string
	Service         string
	Component       string
	Operation       string
	Phase           Phase
	Path            string
	Method          string
	ClientIP        string
	RequestID       string
	UserAgent       string
	Host            string
	AwaitTimeout    time.Duration
	Now             time.Time
	DisableLogging  bool
	LogSampleRate   float64
	ForceLogOnError bool
}

// RequestContext captures core request metadata.
type RequestContext struct {
	RequestID string                 `json:"request_id"`
	ClientIP  string                 `json:"client_ip"`
	UserID    string                 `json:"user_id"`
	Operator  string                 `json:"operator"`
	Method    string                 `json:"http_method"`
	Path      string                 `json:"http_path"`
	Status    int                    `json:"http_status"`
	Extra     map[string]interface{} `json:"extra,omitempty"`
}

// BusinessMetrics summarizes overall business outcome for a request.
type BusinessMetrics struct {
	Operation       string                 `json:"operation"`
	TotalDurationMs float64                `json:"total_duration_ms"`
	OverallStatus   string                 `json:"overall_status"`
	BusinessCode    string                 `json:"business_code"`
	BusinessMessage string                 `json:"business_message"`
	Summary         map[string]interface{} `json:"performance_summary,omitempty"`
}

// Span represents a single timed operation within a trace.
type Span struct {
	ID           string                 `json:"span_id"`
	ParentID     string                 `json:"parent_id"`
	Component    string                 `json:"component"`
	Operation    string                 `json:"operation"`
	StartTime    time.Time              `json:"start_time"`
	EndTime      time.Time              `json:"end_time"`
	DurationMs   float64                `json:"duration_ms"`
	Status       string                 `json:"status"`
	BusinessCode string                 `json:"business_code"`
	Details      map[string]interface{} `json:"details,omitempty"`
	Metrics      []string               `json:"prometheus_metrics,omitempty"`
}

type spanLogEntry struct {
	SpanID       string                 `json:"span_id"`
	ParentID     string                 `json:"parent_id"`
	Component    string                 `json:"component"`
	Operation    string                 `json:"operation"`
	StartTimeMs  int64                  `json:"start_time"`
	EndTimeMs    int64                  `json:"end_time"`
	DurationMs   float64                `json:"duration_ms"`
	Status       string                 `json:"status"`
	BusinessCode string                 `json:"business_code,omitempty"`
	Details      map[string]interface{} `json:"details,omitempty"`
	PromMetrics  []string               `json:"prometheus_metric,omitempty"`
}

type performanceSummary struct {
	APIProcessingMs     float64                `json:"api_processing_ms,omitempty"`
	KafkaProductionMs   float64                `json:"kafka_production_ms,omitempty"`
	KafkaConsumptionMs  float64                `json:"kafka_consumption_ms,omitempty"`
	AdditionalSummaries map[string]interface{} `json:"additional,omitempty"`
}

type businessMetricsPayload struct {
	Operation          string             `json:"operation"`
	TotalDurationMs    float64            `json:"total_duration_ms"`
	OverallStatus      string             `json:"overall_status"`
	BusinessCode       string             `json:"business_code"`
	BusinessMessage    string             `json:"business_message"`
	PerformanceSummary performanceSummary `json:"performance_summary"`
}

type errorCategorySummary struct {
	Validation int            `json:"validation"`
	Database   int            `json:"database"`
	Kafka      int            `json:"kafka"`
	Redis      int            `json:"redis"`
	Business   int            `json:"business"`
	Others     map[string]int `json:"others,omitempty"`
}

type errorAnalysisPayload struct {
	TotalErrors        int                  `json:"total_errors"`
	DegradedOperations int                  `json:"degraded_operations"`
	Categories         errorCategorySummary `json:"error_categories"`
}

type callChainPayload struct {
	RootOperation string         `json:"root_operation"`
	StartTimeMs   int64          `json:"start_time"`
	EndTimeMs     int64          `json:"end_time"`
	Spans         []spanLogEntry `json:"spans"`
}

type traceLogPayload struct {
	TraceID        string                 `json:"trace_id"`
	Level          string                 `json:"level"`
	Timestamp      string                 `json:"timestamp"`
	Service        string                 `json:"service"`
	Component      string                 `json:"component"`
	CallChain      callChainPayload       `json:"call_chain"`
	RequestContext RequestContext         `json:"request_context"`
	Business       businessMetricsPayload `json:"business_metrics"`
	Errors         errorAnalysisPayload   `json:"error_analysis"`
}

// Trace captures spans and metadata for a single logical request or async flow.
type Trace struct {
	ID        string
	Service   string
	Component string
	Operation string
	Phase     Phase

	Start time.Time
	End   time.Time

	RequestContext  RequestContext
	BusinessMetrics BusinessMetrics

	level           string
	status          string
	businessCode    string
	businessMsg     string
	httpStatusCode  int
	logEnabled      bool
	logSampleRate   float64
	forceLogOnError bool
	logDecisionOnce sync.Once
	logEmitDecision bool

	mu    sync.Mutex
	spans []*Span

	asyncExpected bool
	asyncDeadline time.Time
}

type traceContextKey struct{}

type currentSpanKey struct{}

// Start attaches a new Trace to the provided context and returns the derived context and Trace pointer.
func Start(ctx context.Context, opts Options) (context.Context, *Trace) {
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}
	traceID := opts.TraceID
	if traceID == "" {
		traceID = uuid.Must(uuid.NewV4()).String()
	}

	t := &Trace{
		ID:        traceID,
		Service:   opts.Service,
		Component: opts.Component,
		Operation: opts.Operation,
		Phase:     opts.Phase,
		Start:     opts.Now,
		RequestContext: RequestContext{
			RequestID: nonEmpty(opts.RequestID, traceID),
			ClientIP:  opts.ClientIP,
			Method:    opts.Method,
			Path:      opts.Path,
		},
		BusinessMetrics: BusinessMetrics{
			Operation: opts.Operation,
		},
		level:  "INFO",
		status: "success",
	}

	sample := opts.LogSampleRate
	t.logEnabled = !opts.DisableLogging
	if t.logEnabled {
		if sample <= 0 {
			sample = 1
		} else if sample > 1 {
			sample = 1
		}
	} else {
		sample = 0
	}
	t.logSampleRate = sample
	t.forceLogOnError = opts.ForceLogOnError

	if opts.AwaitTimeout > 0 {
		t.asyncDeadline = opts.Now.Add(opts.AwaitTimeout)
	}

	ctx = context.WithValue(ctx, traceContextKey{}, t)
	return ctx, t
}

// FromContext retrieves the Trace from context if present.
func FromContext(ctx context.Context) *Trace {
	if ctx == nil {
		return nil
	}
	if v := ctx.Value(traceContextKey{}); v != nil {
		if t, ok := v.(*Trace); ok {
			return t
		}
	}
	return nil
}

// TraceIDFromContext returns the trace identifier if available.
func TraceIDFromContext(ctx context.Context) string {
	if t := FromContext(ctx); t != nil {
		return t.ID
	}
	return ""
}

// StartSpan starts a new span under the current trace.
func StartSpan(ctx context.Context, component, operation string) (context.Context, *Span) {
	t := FromContext(ctx)
	if t == nil {
		return ctx, nil
	}

	spanID := uuid.Must(uuid.NewV4()).String()
	parentID := ""
	if v := ctx.Value(currentSpanKey{}); v != nil {
		if parent, ok := v.(string); ok {
			parentID = parent
		}
	}

	span := &Span{
		ID:        spanID,
		ParentID:  parentID,
		Component: component,
		Operation: operation,
		StartTime: time.Now(),
		Status:    "success",
	}

	t.mu.Lock()
	t.spans = append(t.spans, span)
	t.mu.Unlock()

	spanCtx := context.WithValue(ctx, currentSpanKey{}, spanID)
	return spanCtx, span
}

// StartSpanWithParent starts a new span and explicitly sets the parent span ID when provided.
func StartSpanWithParent(ctx context.Context, component, operation, parentID string) (context.Context, *Span) {
	t := FromContext(ctx)
	if t == nil {
		return ctx, nil
	}

	spanID := uuid.Must(uuid.NewV4()).String()
	if parentID == "" {
		if v := ctx.Value(currentSpanKey{}); v != nil {
			if parent, ok := v.(string); ok {
				parentID = parent
			}
		}
	}

	span := &Span{
		ID:        spanID,
		ParentID:  parentID,
		Component: component,
		Operation: operation,
		StartTime: time.Now(),
		Status:    "success",
	}

	t.mu.Lock()
	t.spans = append(t.spans, span)
	t.mu.Unlock()

	spanCtx := context.WithValue(ctx, currentSpanKey{}, spanID)
	return spanCtx, span
}

// EndSpan finalizes the span with status and details.
func EndSpan(span *Span, status, businessCode string, details map[string]interface{}, metricsLabels ...string) {
	if span == nil {
		return
	}
	span.EndTime = time.Now()
	span.DurationMs = float64(span.EndTime.Sub(span.StartTime)) / float64(time.Millisecond)
	if status != "" {
		span.Status = status
	}
	if businessCode != "" {
		span.BusinessCode = businessCode
	}
	if details != nil {
		span.Details = details
	}
	if len(metricsLabels) > 0 {
		span.Metrics = append(span.Metrics, metricsLabels...)
	}

	metrics.RecordTraceSpanDuration(span.Component, span.Operation, span.Status, span.EndTime.Sub(span.StartTime))
}

// RecordOutcome captures the business outcome for the trace.
func RecordOutcome(ctx context.Context, businessCode, message, status string, httpStatus int) {
	t := FromContext(ctx)
	if t == nil {
		return
	}

	if status == "" {
		status = "success"
	}
	if businessCode == "" {
		businessCode = strconv.Itoa(code.ErrUnknown)
	}

	t.mu.Lock()
	t.status = status
	t.businessCode = businessCode
	t.businessMsg = message
	t.httpStatusCode = httpStatus
	if status == "error" {
		t.level = "ERROR"
	} else if status == "degraded" && t.level != "ERROR" {
		t.level = "WARN"
	}
	t.mu.Unlock()
}

// SetOperator records the operator or user identifier on the trace.
func SetOperator(ctx context.Context, operator string) {
	t := FromContext(ctx)
	if t == nil || operator == "" {
		return
	}
	t.mu.Lock()
	t.RequestContext.Operator = operator
	if t.RequestContext.UserID == "" {
		t.RequestContext.UserID = operator
	}
	t.mu.Unlock()
}

// SetUserID records the acting user id independently.
func SetUserID(ctx context.Context, userID string) {
	t := FromContext(ctx)
	if t == nil || userID == "" {
		return
	}
	t.mu.Lock()
	t.RequestContext.UserID = userID
	t.mu.Unlock()
}

// AddRequestTag attaches arbitrary metadata to the request context for downstream logging.
func AddRequestTag(ctx context.Context, key string, value interface{}) {
	if key == "" {
		return
	}
	t := FromContext(ctx)
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.RequestContext.Extra == nil {
		t.RequestContext.Extra = make(map[string]interface{})
	}
	t.RequestContext.Extra[key] = value
	t.mu.Unlock()
}

// GetRequestTag retrieves a request-scoped tag previously stored on the trace.
func GetRequestTag(ctx context.Context, key string) (interface{}, bool) {
	if key == "" {
		return nil, false
	}
	t := FromContext(ctx)
	if t == nil {
		return nil, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.RequestContext.Extra == nil {
		return nil, false
	}
	v, ok := t.RequestContext.Extra[key]
	return v, ok
}

// UpdateHTTPStatus records the latest HTTP status code and escalates outcome metadata when needed.
func UpdateHTTPStatus(ctx context.Context, httpStatus int) {
	t := FromContext(ctx)
	if t == nil {
		return
	}
	t.mu.Lock()
	t.httpStatusCode = httpStatus
	if t.RequestContext.Status == 0 {
		t.RequestContext.Status = httpStatus
	}
	if httpStatus >= http.StatusInternalServerError {
		t.status = "error"
		t.level = "ERROR"
	} else if httpStatus >= http.StatusBadRequest && t.status != "error" {
		t.status = "degraded"
		if t.level == "INFO" {
			t.level = "WARN"
		}
	}
	t.mu.Unlock()
}

// ExpectAsync marks the trace as expecting asynchronous completion.
func ExpectAsync(ctx context.Context, deadline time.Time) {
	t := FromContext(ctx)
	if t == nil {
		return
	}
	t.mu.Lock()
	t.asyncExpected = true
	if !deadline.IsZero() {
		t.asyncDeadline = deadline
	}
	t.mu.Unlock()
}

// Complete marks the trace phase as complete and forwards to the collector for emission.
func Complete(ctx context.Context) {
	t := FromContext(ctx)
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.End.IsZero() {
		t.End = time.Now()
	}
	t.RequestContext.Status = t.httpStatusCode
	t.BusinessMetrics.BusinessCode = t.businessCode
	t.BusinessMetrics.BusinessMessage = t.businessMsg
	t.BusinessMetrics.OverallStatus = t.status
	t.BusinessMetrics.TotalDurationMs = float64(t.End.Sub(t.Start)) / float64(time.Millisecond)
	t.mu.Unlock()

	defaultCollector.Enqueue(t)
}

// NewDetached creates a trace for asynchronous stages without inheriting an existing context.
func NewDetached(opts Options) (*Trace, context.Context) {
	ctx := context.Background()
	opts.Phase = PhaseAsync
	ctx, t := Start(ctx, opts)
	return t, ctx
}

// MarshalSpans returns ordered spans for logging.
func (t *Trace) MarshalSpans() []*Span {
	t.mu.Lock()
	defer t.mu.Unlock()
	spans := make([]*Span, len(t.spans))
	copy(spans, t.spans)
	sort.Slice(spans, func(i, j int) bool {
		return spans[i].StartTime.Before(spans[j].StartTime)
	})
	return spans
}

// ToLogPayload builds the final log message payload.
func (t *Trace) ToLogPayload(asyncSpans []*Span) traceLogPayload {
	spans := t.MarshalSpans()
	if len(asyncSpans) > 0 {
		spans = append(spans, asyncSpans...)
		sort.Slice(spans, func(i, j int) bool {
			return spans[i].StartTime.Before(spans[j].StartTime)
		})
	}

	spanPayload := make([]spanLogEntry, 0, len(spans))
	errorCategories := map[string]int{
		"validation": 0,
		"database":   0,
		"kafka":      0,
		"redis":      0,
		"business":   0,
	}
	degraded := 0
	errors := 0

	errorSpanIDs := make(map[string]struct{})
	errorParentIDs := make(map[string]struct{})
	for _, span := range spans {
		if span.Status == "error" {
			errorSpanIDs[span.ID] = struct{}{}
		}
	}
	for _, span := range spans {
		if span.Status == "error" {
			if _, ok := errorSpanIDs[span.ParentID]; ok {
				errorParentIDs[span.ParentID] = struct{}{}
			}
		}
	}

	for _, span := range spans {
		entry := spanLogEntry{
			SpanID:      span.ID,
			ParentID:    span.ParentID,
			Component:   span.Component,
			Operation:   span.Operation,
			StartTimeMs: span.StartTime.UnixNano() / int64(time.Millisecond),
			EndTimeMs:   span.EndTime.UnixNano() / int64(time.Millisecond),
			DurationMs:  span.DurationMs,
			Status:      span.Status,
		}
		if span.BusinessCode != "" {
			entry.BusinessCode = span.BusinessCode
		}
		if len(span.Metrics) > 0 {
			entry.PromMetrics = append(entry.PromMetrics, span.Metrics...)
		}
		if len(span.Details) > 0 {
			entry.Details = span.Details
			categorizeError(span, errorCategories)
		}
		spanPayload = append(spanPayload, entry)
		if span.Status == "error" {
			if _, hasErrorChild := errorParentIDs[span.ID]; !hasErrorChild {
				errors++
			}
		} else if span.Status == "degraded" {
			degraded++
		}
	}

	var level = t.level
	if errors > 0 {
		level = "ERROR"
	} else if degraded > 0 && level == "INFO" {
		level = "WARN"
	}

	performance := performanceSummary{
		APIProcessingMs:    estimateComponentDuration(spans, "user-controller") + estimateComponentDuration(spans, "user-service"),
		KafkaProductionMs:  estimateComponentDuration(spans, "kafka-producer"),
		KafkaConsumptionMs: estimateComponentDuration(spans, "kafka-consumer"),
	}
	if len(t.BusinessMetrics.Summary) > 0 {
		performance.AdditionalSummaries = t.BusinessMetrics.Summary
	}

	errorSummary := errorCategorySummary{
		Validation: errorCategories["validation"],
		Database:   errorCategories["database"],
		Kafka:      errorCategories["kafka"],
		Redis:      errorCategories["redis"],
		Business:   errorCategories["business"],
	}
	for key, value := range errorCategories {
		switch key {
		case "validation", "database", "kafka", "redis", "business":
			continue
		default:
			if errorSummary.Others == nil {
				errorSummary.Others = make(map[string]int)
			}
			errorSummary.Others[key] = value
		}
	}

	return traceLogPayload{
		TraceID:   t.ID,
		Level:     level,
		Timestamp: t.End.UTC().Format(time.RFC3339Nano),
		Service:   t.Service,
		Component: t.Component,
		CallChain: callChainPayload{
			RootOperation: t.Operation,
			StartTimeMs:   t.Start.UnixNano() / int64(time.Millisecond),
			EndTimeMs:     t.End.UnixNano() / int64(time.Millisecond),
			Spans:         spanPayload,
		},
		RequestContext: t.RequestContext,
		Business: businessMetricsPayload{
			Operation:          t.BusinessMetrics.Operation,
			TotalDurationMs:    t.BusinessMetrics.TotalDurationMs,
			OverallStatus:      t.BusinessMetrics.OverallStatus,
			BusinessCode:       t.BusinessMetrics.BusinessCode,
			BusinessMessage:    t.BusinessMetrics.BusinessMessage,
			PerformanceSummary: performance,
		},
		Errors: errorAnalysisPayload{
			TotalErrors:        errors,
			DegradedOperations: degraded,
			Categories:         errorSummary,
		},
	}
}

// Log emits the trace payload using structured logging.
func (t *Trace) Log(asyncSpans []*Span) {
	if !t.shouldLog() {
		return
	}
	payload := t.ToLogPayload(asyncSpans)
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		log.Errorw("trace marshal failure", "traceID", t.ID, "error", err)
		return
	}
	if t.level == "ERROR" {
		log.Error(string(payloadJSON))
	} else if t.level == "WARN" {
		log.Warn(string(payloadJSON))
	} else {
		log.Info(string(payloadJSON))
	}

	metrics.RecordTraceOperationDuration(t.Operation, string(t.Phase), t.status, t.End.Sub(t.Start))
}

func nonEmpty(primary, fallback string) string {
	if primary != "" {
		return primary
	}
	return fallback
}

func categorizeError(span *Span, categories map[string]int) {
	if span == nil || len(span.Details) == 0 {
		return
	}
	if domain, ok := span.Details["error_category"].(string); ok {
		if domain != "" {
			categories[domain]++
		}
	}
}

func (t *Trace) shouldLog() bool {
	if t.forceLogOnError && t.status == "error" {
		return true
	}
	t.logDecisionOnce.Do(func() {
		if !t.logEnabled {
			t.logEmitDecision = false
			return
		}
		if t.logSampleRate >= 1 {
			t.logEmitDecision = true
			return
		}
		if t.logSampleRate <= 0 {
			t.logEmitDecision = false
			return
		}
		t.logEmitDecision = rand.Float64() < t.logSampleRate
	})
	return t.logEmitDecision
}

func estimateComponentDuration(spans []*Span, component string) float64 {
	var total float64
	for _, span := range spans {
		if span.Component == component {
			total += span.DurationMs
		}
	}
	return total
}

// Collector coordinates trace emission across HTTP and async phases.
type Collector struct {
	mu           sync.Mutex
	traces       map[string]*collectorEntry
	awaitTimeout time.Duration
}

type collectorEntry struct {
	httpTrace  *Trace
	asyncSpans []*Span
	asyncReady bool
	timer      *time.Timer
}

var defaultCollector = NewCollector()

// NewCollector constructs a collector with a default timeout.
func NewCollector() *Collector {
	return &Collector{
		traces:       make(map[string]*collectorEntry),
		awaitTimeout: 30 * time.Second,
	}
}

// SetAwaitTimeout configures the waiting timeout.
func (c *Collector) SetAwaitTimeout(timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	c.mu.Lock()
	c.awaitTimeout = timeout
	c.mu.Unlock()
}

// Enqueue registers a completed trace phase.
func (c *Collector) Enqueue(t *Trace) {
	if t == nil {
		return
	}

	if t.Phase == PhaseAsync {
		c.mergeAsync(t)
		return
	}

	c.mergeHTTP(t)
}

func (c *Collector) mergeHTTP(t *Trace) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.traces[t.ID]
	if !exists {
		entry = &collectorEntry{}
		c.traces[t.ID] = entry
	}
	entry.httpTrace = t

	if entry.asyncReady || !t.asyncExpected {
		asyncSpans := entry.asyncSpans
		delete(c.traces, t.ID)
		c.mu.Unlock()
		t.Log(asyncSpans)
		c.mu.Lock()
		return
	}

	// Wait for async completion, schedule timeout
	timeout := c.awaitTimeout
	if !t.asyncDeadline.IsZero() {
		if deadlineDelta := time.Until(t.asyncDeadline); deadlineDelta > 0 {
			timeout = deadlineDelta
		}
	}

	if timeout <= 0 {
		asyncSpans := entry.asyncSpans
		delete(c.traces, t.ID)
		c.mu.Unlock()
		t.Log(asyncSpans)
		c.mu.Lock()
		return
	}

	entry.timer = time.AfterFunc(timeout, func() {
		c.flushTimeout(t.ID)
	})
}

func (c *Collector) mergeAsync(t *Trace) {
	spans := t.MarshalSpans()
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.traces[t.ID]
	if !exists {
		entry = &collectorEntry{}
		c.traces[t.ID] = entry
	}
	entry.asyncSpans = append(entry.asyncSpans, spans...)
	if !t.asyncExpected {
		entry.asyncReady = true
	}

	if entry.httpTrace != nil && entry.asyncReady {
		if entry.timer != nil {
			entry.timer.Stop()
		}
		httpTrace := entry.httpTrace
		async := entry.asyncSpans
		delete(c.traces, t.ID)
		c.mu.Unlock()
		httpTrace.Log(async)
		c.mu.Lock()
	}
}

func (c *Collector) flushTimeout(traceID string) {
	c.mu.Lock()
	entry, exists := c.traces[traceID]
	if !exists {
		c.mu.Unlock()
		return
	}
	if entry.httpTrace == nil {
		delete(c.traces, traceID)
		c.mu.Unlock()
		return
	}

	httpTrace := entry.httpTrace
	async := entry.asyncSpans
	delete(c.traces, traceID)
	c.mu.Unlock()

	httpTrace.Log(async)
}
