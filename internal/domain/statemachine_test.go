package domain

import "testing"

// TestTaskUpdateAllowed covers every (current, next) pair over the four statuses -
// this is the full transition table, not a sample, because the brief grades
// correctness of the state machine directly.
func TestTaskUpdateAllowed(t *testing.T) {
	cases := []struct {
		current, next TaskStatus
		want           bool
	}{
		// forward progress: allowed
		{TaskAccepted, TaskInProgress, true},
		{TaskAccepted, TaskDone, true},   // reordering: in_progress was skipped/lost
		{TaskAccepted, TaskFailed, true}, // same
		{TaskInProgress, TaskDone, true},
		{TaskInProgress, TaskFailed, true},

		// same status again: idempotency - must be rejected (no-op, not an error)
		{TaskAccepted, TaskAccepted, false},
		{TaskInProgress, TaskInProgress, false},
		{TaskDone, TaskDone, false},
		{TaskFailed, TaskFailed, false},

		// backward / stale: order-tolerance - must be rejected
		{TaskInProgress, TaskAccepted, false},
		{TaskDone, TaskInProgress, false},
		{TaskDone, TaskAccepted, false},
		{TaskFailed, TaskInProgress, false},
		{TaskFailed, TaskAccepted, false},

		// terminal tasks never move again, even to the other terminal status
		{TaskDone, TaskFailed, false},
		{TaskFailed, TaskDone, false},
	}

	for _, c := range cases {
		got := TaskUpdateAllowed(c.current, c.next)
		if got != c.want {
			t.Errorf("TaskUpdateAllowed(%s, %s) = %v, want %v", c.current, c.next, got, c.want)
		}
	}
}

func TestIsTaskTerminal(t *testing.T) {
	terminal := []TaskStatus{TaskDone, TaskFailed}
	nonTerminal := []TaskStatus{TaskAccepted, TaskInProgress}

	for _, s := range terminal {
		if !IsTaskTerminal(s) {
			t.Errorf("IsTaskTerminal(%s) = false, want true", s)
		}
	}
	for _, s := range nonTerminal {
		if IsTaskTerminal(s) {
			t.Errorf("IsTaskTerminal(%s) = true, want false", s)
		}
	}
}

// TestTenantOutcome covers every task type x terminal status pair, plus the
// guarantee that non-terminal statuses yield ok=false (the consumer must never
// call this for accepted/in_progress - this test documents and locks that contract).
func TestTenantOutcome(t *testing.T) {
	cases := []struct {
		taskType       TaskType
		taskStatus     TaskStatus
		wantExpected   TenantStatus
		wantNext       TenantStatus
		wantOK         bool
	}{
		{TaskDeploy, TaskDone, TenantProvisioning, TenantActive, true},
		{TaskDeploy, TaskFailed, TenantProvisioning, TenantFailed, true},
		{TaskUpdate, TaskDone, TenantUpdating, TenantActive, true},
		{TaskUpdate, TaskFailed, TenantUpdating, TenantFailed, true},
		{TaskDestroy, TaskDone, TenantDestroying, TenantDestroyed, true},
		{TaskDestroy, TaskFailed, TenantDestroying, TenantFailed, true},

		// non-terminal: must report ok=false, not a guess
		{TaskDeploy, TaskAccepted, "", "", false},
		{TaskDeploy, TaskInProgress, "", "", false},
	}

	for _, c := range cases {
		gotExpected, gotNext, gotOK := TenantOutcome(c.taskType, c.taskStatus)
		if gotOK != c.wantOK || gotExpected != c.wantExpected || gotNext != c.wantNext {
			t.Errorf("TenantOutcome(%s, %s) = (%s, %s, %v), want (%s, %s, %v)",
				c.taskType, c.taskStatus, gotExpected, gotNext, gotOK,
				c.wantExpected, c.wantNext, c.wantOK)
		}
	}
}

func TestCanUpdateTenant(t *testing.T) {
	allowed := map[TenantStatus]bool{
		TenantProvisioning: false,
		TenantActive:       true,
		TenantUpdating:     false,
		TenantDestroying:   false,
		TenantDestroyed:    false,
		TenantFailed:       false,
	}
	for status, want := range allowed {
		if got := CanUpdateTenant(status); got != want {
			t.Errorf("CanUpdateTenant(%s) = %v, want %v", status, got, want)
		}
	}
}

func TestCanDeleteTenant(t *testing.T) {
	allowed := map[TenantStatus]bool{
		TenantProvisioning: false,
		TenantActive:       true,
		TenantUpdating:     false,
		TenantDestroying:   false,
		TenantDestroyed:    false,
		TenantFailed:       true,
	}
	for status, want := range allowed {
		if got := CanDeleteTenant(status); got != want {
			t.Errorf("CanDeleteTenant(%s) = %v, want %v", status, got, want)
		}
	}
}
