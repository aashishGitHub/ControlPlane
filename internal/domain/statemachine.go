package domain

// --- Task state machine (DESIGN.md §3) ---------------------------------------------

type TaskStatus string

const (
	TaskAccepted   TaskStatus = "accepted"
	TaskInProgress TaskStatus = "in_progress"
	TaskDone       TaskStatus = "done"
	TaskFailed     TaskStatus = "failed"
)

type TaskType string

const (
	TaskDeploy  TaskType = "deploy"
	TaskUpdate  TaskType = "update"
	TaskDestroy TaskType = "destroy"
)

// taskRank gives every status a position on a single forward-only line:
// accepted(0) -> in_progress(1) -> {done,failed}(2, terminal).
//
// Pattern: rank-based transition guard (Decisions table row 4, DESIGN.md §7).
// Why: a worker-progress update should be applied if and only if it moves the task
// strictly forward. Encoding that as "rank(new) > rank(current)" gives idempotency
// (rank(x) > rank(x) is false - a repeat of the same status never re-applies) and
// order-tolerance (a reordered lower-rank update, e.g. a late in_progress arriving
// after done, is rejected) from one comparison, instead of enumerating every
// from->to pair and every "stale" pair separately.
var taskRank = map[TaskStatus]int{
	TaskAccepted:   0,
	TaskInProgress: 1,
	TaskDone:       2,
	TaskFailed:     2,
}

// TaskUpdateAllowed reports whether a worker-reported status transition from
// current to next should be applied. See the comment on taskRank for why this is
// a single rank comparison rather than a from->to lookup table.
func TaskUpdateAllowed(current, next TaskStatus) bool {
	return taskRank[next] > taskRank[current]
}

// IsTaskTerminal reports whether a task status is final (done or failed). A
// terminal task's status can never change again - any further update for it is
// stale by construction, since nothing outranks rank 2.
func IsTaskTerminal(status TaskStatus) bool {
	return status == TaskDone || status == TaskFailed
}

// --- Tenant state machine (DESIGN.md §3) --------------------------------------------

type TenantStatus string

const (
	TenantProvisioning TenantStatus = "provisioning"
	TenantActive       TenantStatus = "active"
	TenantUpdating     TenantStatus = "updating"
	TenantDestroying   TenantStatus = "destroying"
	TenantDestroyed    TenantStatus = "destroyed"
	TenantFailed       TenantStatus = "failed"
)

// taskTypeStart maps a task type to the tenant status it puts the tenant into
// when the task is first created ("accepted"). Used by the service layer, not by
// the consumer - the consumer only needs the terminal-outcome mapping below.
var taskTypeStart = map[TaskType]TenantStatus{
	TaskDeploy:  TenantProvisioning,
	TaskUpdate:  TenantUpdating,
	TaskDestroy: TenantDestroying,
}

// StartingTenantStatus returns the tenant status that creating a task of the given
// type puts the tenant into.
func StartingTenantStatus(t TaskType) TenantStatus {
	return taskTypeStart[t]
}

// expectedInFlightStatus is the tenant status a task's tenant must currently be in
// for that task's terminal outcome to apply. It doubles as the optimistic-locking
// guard in the UPDATE statement that applies the outcome (DESIGN.md §6 Inbound,
// step 5): "WHERE status = expectedInFlightStatus(type)". If the tenant has moved
// on for any reason, the UPDATE matches zero rows and the stale outcome is dropped
// rather than corrupting a newer state - the same zero-rows-means-stop idiom as the
// API's optimistic locking (DESIGN.md §5).
var expectedInFlightStatus = map[TaskType]TenantStatus{
	TaskDeploy:  TenantProvisioning,
	TaskUpdate:  TenantUpdating,
	TaskDestroy: TenantDestroying,
}

// doneOutcome is the tenant status a successful (done) task of the given type
// drives the tenant to.
var doneOutcome = map[TaskType]TenantStatus{
	TaskDeploy:  TenantActive,
	TaskUpdate:  TenantActive,
	TaskDestroy: TenantDestroyed,
}

// TenantOutcome returns the tenant status a terminal task outcome should produce,
// along with the in-flight status the tenant is expected to currently hold (the
// optimistic-locking guard). ok is false if taskStatus is not terminal - callers
// must not call this for accepted/in_progress updates.
func TenantOutcome(taskType TaskType, taskStatus TaskStatus) (expectedCurrent, next TenantStatus, ok bool) {
	if !IsTaskTerminal(taskStatus) {
		return "", "", false
	}
	expectedCurrent = expectedInFlightStatus[taskType]
	if taskStatus == TaskFailed {
		return expectedCurrent, TenantFailed, true
	}
	return expectedCurrent, doneOutcome[taskType], true
}

// CanUpdateTenant reports whether a PATCH is allowed from the given tenant status.
// Per the brief: only from "active".
func CanUpdateTenant(status TenantStatus) bool {
	return status == TenantActive
}

// CanDeleteTenant reports whether a DELETE is allowed from the given tenant status.
// Per the brief: from "active" or "failed" (a tenant stuck in failed can still be
// torn down).
func CanDeleteTenant(status TenantStatus) bool {
	return status == TenantActive || status == TenantFailed
}
