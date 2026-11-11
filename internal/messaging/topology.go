package messaging

import (
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/aashishGithub/controlplane/internal/domain"
)

const (
	ExchangeTasks        = "cp.tasks"
	QueueWorkerTasks     = "worker.tasks"
	ExchangeTaskUpdates  = "cp.task-updates"
	QueueTaskUpdates     = "cp.task-updates"
	QueueTaskUpdatesDLQ  = "cp.task-updates.dlq"
	taskUpdateRoutingKey = "task.update."
)

// taskTypes and taskStatuses are the binding sets for the two direct exchanges.
// Direct exchanges are strict: every routing key a producer can emit must have an
// explicit binding, or the broker confirms receipt and silently drops the message
// (DESIGN.md §6 / M3 QnA Q45).
var (
	taskTypes = []domain.TaskType{
		domain.TaskDeploy,
		domain.TaskUpdate,
		domain.TaskDestroy,
	}
	taskStatuses = []domain.TaskStatus{
		domain.TaskAccepted,
		domain.TaskInProgress,
		domain.TaskDone,
		domain.TaskFailed,
	}
)

// DeclareTopology declares the full AMQP topology. It is idempotent when the
// arguments match exactly, and must run at startup and after every reconnect
// (DESIGN.md §6 / §7).
//
// Queue settings note: durable is positional; x-queue-type, x-delivery-limit, and
// DLX settings are table arguments. Quorum queues require durable=true - putting
// "durable" in the table and passing false is a precondition failure.
func DeclareTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(ExchangeTasks, "direct", true, false, false, false, nil); err != nil {
		return err
	}
	quorum := amqp.Table{"x-queue-type": "quorum"}
	if _, err := ch.QueueDeclare(QueueWorkerTasks, true, false, false, false, quorum); err != nil {
		return err
	}
	for _, taskType := range taskTypes {
		if err := ch.QueueBind(QueueWorkerTasks, TaskRoutingKey(string(taskType)), ExchangeTasks, false, nil); err != nil {
			return err
		}
	}

	if err := ch.ExchangeDeclare(ExchangeTaskUpdates, "direct", true, false, false, false, nil); err != nil {
		return err
	}
	updatesArgs := amqp.Table{
		"x-queue-type":              "quorum",
		"x-delivery-limit":          int32(5),
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": QueueTaskUpdatesDLQ,
	}
	if _, err := ch.QueueDeclare(QueueTaskUpdates, true, false, false, false, updatesArgs); err != nil {
		return err
	}
	for _, status := range taskStatuses {
		if err := ch.QueueBind(QueueTaskUpdates, taskUpdateRoutingKey+string(status), ExchangeTaskUpdates, false, nil); err != nil {
			return err
		}
	}
	if _, err := ch.QueueDeclare(QueueTaskUpdatesDLQ, true, false, false, false, nil); err != nil {
		return err
	}
	return nil
}
