package service

import (
	"context"

	"github.com/google/uuid"

	"github.com/aashishGithub/controlplane/internal/domain"
	"github.com/aashishGithub/controlplane/internal/store"
)

func (s *Service) GetTask(ctx context.Context, id uuid.UUID) (store.Task, error) {
	t, err := store.GetTaskByID(ctx, s.pool, id)
	if err == store.ErrNoRows {
		return store.Task{}, domain.NewError(domain.ErrTaskNotFound, "task not found")
	}
	return t, err
}

func (s *Service) ListTasks(ctx context.Context, status, taskType string, cursor uuid.UUID, limit int) ([]store.Task, error) {
	return store.ListTasks(ctx, s.pool, status, taskType, cursor, limit)
}
