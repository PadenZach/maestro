package api

// These models are both the JSON responses and the source of their OpenAPI schemas.
type Workflow struct {
	WorkflowID        string  `json:"workflowId" minLength:"1"`
	Status            string  `json:"status" minLength:"1"`
	WorkflowName      *string `json:"workflowName"`
	WorkflowClass     *string `json:"workflowClass"`
	WorkflowConfig    *string `json:"workflowConfig"`
	User              *string `json:"user"`
	AssumedRole       *string `json:"assumedRole"`
	Roles             *string `json:"roles"`
	Input             *string `json:"input"`
	Output            *string `json:"output"`
	Error             *string `json:"error"`
	QueueName         *string `json:"queueName"`
	AppVersion        *string `json:"appVersion"`
	ExecutorID        *string `json:"executorId"`
	DeduplicationID   *string `json:"deduplicationId"`
	QueuePartitionKey *string `json:"queuePartitionKey"`
	ForkedFrom        *string `json:"forkedFrom"`
	WasForkedFrom     bool    `json:"wasForkedFrom"`
	ParentWorkflowID  *string `json:"parentWorkflowId"`
	Attributes        *string `json:"attributes"`
	ScheduleName      *string `json:"scheduleName"`
	ApplicationName   *string `json:"applicationName"`
	CreatedAt         *string `json:"createdAt" format:"date-time" nullable:"false"`
	UpdatedAt         *string `json:"updatedAt" format:"date-time"`
	Deadline          *string `json:"deadline" format:"date-time"`
	DequeuedAt        *string `json:"dequeuedAt" format:"date-time"`
	DelayUntil        *string `json:"delayUntil" format:"date-time"`
	CompletedAt       *string `json:"completedAt" format:"date-time"`
	Priority          *int64  `json:"priority" format:"int32" minimum:"-2147483648" maximum:"2147483647"`
	TimeoutMS         *int64  `json:"timeoutMs"`
}

type Step struct {
	StepID          int     `json:"stepId" format:"int32" minimum:"-2147483648" maximum:"2147483647"`
	StepName        string  `json:"stepName" minLength:"1"`
	Output          *string `json:"output"`
	Error           *string `json:"error"`
	ChildWorkflowID *string `json:"childWorkflowId"`
	StartedAt       *string `json:"startedAt" format:"date-time"`
	CompletedAt     *string `json:"completedAt" format:"date-time"`
}

type Queue struct {
	Name                         string   `json:"name"`
	Concurrency                  *int     `json:"concurrency" format:"int32"`
	WorkerConcurrency            *int     `json:"workerConcurrency" format:"int32"`
	RateLimitMax                 *int     `json:"rateLimitMax" format:"int32"`
	RateLimitPeriodSecs          *float64 `json:"rateLimitPeriodSecs"`
	PriorityEnabled              bool     `json:"priorityEnabled"`
	PartitionQueue               bool     `json:"partitionQueue"`
	PollingIntervalSecs          float64  `json:"pollingIntervalSecs"`
	ApplicationName              *string  `json:"applicationName"`
	PartitionConcurrency         *int     `json:"partitionConcurrency" format:"int32"`
	PartitionWorkerConcurrency   *int     `json:"partitionWorkerConcurrency" format:"int32"`
	PartitionRateLimitMax        *int     `json:"partitionRateLimitMax" format:"int32"`
	PartitionRateLimitPeriodSecs *float64 `json:"partitionRateLimitPeriodSecs"`
}

type Schedule struct {
	ScheduleID        string  `json:"scheduleId"`
	ScheduleName      string  `json:"scheduleName"`
	WorkflowName      string  `json:"workflowName"`
	WorkflowClass     *string `json:"workflowClass"`
	CronExpression    string  `json:"cronExpression"`
	Status            string  `json:"status"`
	Context           *string `json:"context"`
	LastFiredAt       *string `json:"lastFiredAt" format:"date-time"`
	AutomaticBackfill bool    `json:"automaticBackfill"`
	CronTimezone      *string `json:"cronTimezone"`
	ApplicationName   *string `json:"applicationName"`
}

type WorkflowAggregate struct {
	Group             map[string]*string `json:"group"`
	Count             *int64             `json:"count,omitempty"`
	MinCreatedAt      *string            `json:"minCreatedAt,omitempty" format:"date-time"`
	MaxQueueWaitMS    *int64             `json:"maxQueueWaitMs,omitempty"`
	MaxTotalLatencyMS *int64             `json:"maxTotalLatencyMs,omitempty"`
}

type StepAggregate struct {
	Group         map[string]*string `json:"group"`
	Count         *int64             `json:"count,omitempty"`
	MaxDurationMS *int64             `json:"maxDurationMs,omitempty"`
}

type ExportWorkflowOutputBody struct {
	SerializedWorkflow string `json:"serializedWorkflow"`
}

type Problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail"`
}

// The parsers use these field sets to reject unsupported request properties.
// SDK wire names and conversions remain in the protocol request builders.
type workflowFilters struct {
	Status           []string       `json:"status,omitempty" nullable:"true"`
	WorkflowName     []string       `json:"workflowName,omitempty" nullable:"true"`
	WorkflowIDs      []string       `json:"workflowIds,omitempty" nullable:"true"`
	WorkflowIDPrefix []string       `json:"workflowIdPrefix,omitempty" nullable:"true"`
	AppVersion       []string       `json:"appVersion,omitempty" nullable:"true"`
	ExecutorID       []string       `json:"executorId,omitempty" nullable:"true"`
	QueueName        []string       `json:"queueName,omitempty" nullable:"true"`
	ForkedFrom       []string       `json:"forkedFrom,omitempty" nullable:"true"`
	ParentWorkflowID []string       `json:"parentWorkflowId,omitempty" nullable:"true"`
	User             []string       `json:"user,omitempty" nullable:"true"`
	ScheduleName     []string       `json:"scheduleName,omitempty" nullable:"true"`
	StartTime        string         `json:"startTime,omitempty" format:"date-time"`
	EndTime          string         `json:"endTime,omitempty" format:"date-time"`
	CompletedAfter   string         `json:"completedAfter,omitempty" format:"date-time"`
	CompletedBefore  string         `json:"completedBefore,omitempty" format:"date-time"`
	DequeuedAfter    string         `json:"dequeuedAfter,omitempty" format:"date-time"`
	DequeuedBefore   string         `json:"dequeuedBefore,omitempty" format:"date-time"`
	WasForkedFrom    bool           `json:"wasForkedFrom,omitempty"`
	HasParent        bool           `json:"hasParent,omitempty"`
	Attributes       map[string]any `json:"attributes,omitempty"`
}

type WorkflowSearchBody struct {
	workflowFilters
	Limit      *int `json:"limit,omitempty" minimum:"0"`
	Offset     *int `json:"offset,omitempty" minimum:"0"`
	SortDesc   bool `json:"sortDesc,omitempty"`
	QueuesOnly bool `json:"queuesOnly,omitempty"`
	LoadInput  bool `json:"loadInput,omitempty"`
	LoadOutput bool `json:"loadOutput,omitempty"`
}

type WorkflowAggregatesBody struct {
	workflowFilters
	GroupByStatus           bool  `json:"groupByStatus,omitempty"`
	GroupByWorkflowName     bool  `json:"groupByWorkflowName,omitempty"`
	GroupByQueueName        bool  `json:"groupByQueueName,omitempty"`
	GroupByExecutorID       bool  `json:"groupByExecutorId,omitempty"`
	GroupByAppVersion       bool  `json:"groupByAppVersion,omitempty"`
	GroupByApplicationName  bool  `json:"groupByApplicationName,omitempty"`
	SelectCount             bool  `json:"selectCount,omitempty"`
	SelectMinCreatedAt      bool  `json:"selectMinCreatedAt,omitempty"`
	SelectMaxQueueWaitMS    bool  `json:"selectMaxQueueWaitMs,omitempty"`
	SelectMaxTotalLatencyMS bool  `json:"selectMaxTotalLatencyMs,omitempty"`
	TimeBucketSizeMS        int64 `json:"timeBucketSizeMs,omitempty"`
}

type StepAggregatesBody struct {
	GroupByFunctionName bool     `json:"groupByFunctionName,omitempty"`
	GroupByStatus       bool     `json:"groupByStatus,omitempty"`
	SelectCount         bool     `json:"selectCount,omitempty"`
	SelectMaxDurationMS bool     `json:"selectMaxDurationMs,omitempty"`
	TimeBucketSizeMS    int64    `json:"timeBucketSizeMs,omitempty"`
	Status              []string `json:"status,omitempty" nullable:"false"`
	StepName            []string `json:"stepName,omitempty" nullable:"false"`
	WorkflowIDPrefix    []string `json:"workflowIdPrefix,omitempty" nullable:"false"`
	CompletedAfter      string   `json:"completedAfter,omitempty" format:"date-time"`
	CompletedBefore     string   `json:"completedBefore,omitempty" format:"date-time"`
}

type workflowListQuery struct {
	Status       string `json:"status,omitempty"`
	WorkflowName string `json:"workflowName,omitempty"`
	Limit        *int   `json:"limit,omitempty" minimum:"0"`
	Offset       *int   `json:"offset,omitempty" minimum:"0"`
	SortDesc     bool   `json:"sortDesc,omitempty"`
	LoadInput    bool   `json:"loadInput,omitempty"`
	LoadOutput   bool   `json:"loadOutput,omitempty"`
}

type scheduleListQuery struct {
	Status             string `json:"status,omitempty"`
	WorkflowName       string `json:"workflowName,omitempty"`
	ScheduleNamePrefix string `json:"scheduleNamePrefix,omitempty"`
	LoadContext        bool   `json:"loadContext,omitempty"`
}

type stepsQuery struct {
	Limit  *int `json:"limit,omitempty" minimum:"0"`
	Offset *int `json:"offset,omitempty" minimum:"0"`
}

type exportQuery struct {
	ExportChildren bool `json:"exportChildren,omitempty"`
}
