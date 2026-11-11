package messaging

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher puts a channel in confirm mode and waits for the broker's ack before
// reporting success.
//
// Pattern: publisher confirms (DESIGN.md §6 Outbound). Why: PublishWithContext
// returning nil only means the bytes reached the socket - not that the broker
// persisted or even routed the message. The outbox relay must mark
// published_at only after the broker takes responsibility; otherwise a crash
// after marking can lose the event (at-most-once).
type Publisher struct {
	ch       *amqp.Channel
	confirms chan amqp.Confirmation
	returns  chan amqp.Return
}

func NewPublisher(ch *amqp.Channel) (*Publisher, error) {
	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("messaging: enable publisher confirms: %w", err)
	}
	return &Publisher{
		ch:       ch,
		confirms: ch.NotifyPublish(make(chan amqp.Confirmation, 1)),
		returns:  ch.NotifyReturn(make(chan amqp.Return, 1)),
	}, nil
}

// PublishTask publishes an outbound task message.
func (p *Publisher) PublishTask(ctx context.Context, msg TaskMsg) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("messaging: marshal task: %w", err)
	}
	return p.publish(ctx, ExchangeTasks, TaskRoutingKey(msg.Type), body)
}

// PublishUpdate publishes an inbound task-update message. The control plane does
// not produce these in normal operation (workers do); it exists for the M3
// routing/consumer tests and the M4 fault-injection scripts.
func (p *Publisher) PublishUpdate(ctx context.Context, msg TaskUpdateMsg) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("messaging: marshal update: %w", err)
	}
	return p.publish(ctx, ExchangeTaskUpdates, UpdateRoutingKey(msg.Status), body)
}

func (p *Publisher) publish(ctx context.Context, exchange, routingKey string, body []byte) error {
	// mandatory=true turns "no queue matched this routing key" from a silent drop
	// into a broker basic.return. A confirm alone does NOT prove routing: RabbitMQ
	// happily confirms an unroutable message (M3 QnA Q5).
	err := p.ch.PublishWithContext(ctx, exchange, routingKey, true, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
	if err != nil {
		return fmt.Errorf("messaging: publish: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case returned := <-p.returns:
		return fmt.Errorf("messaging: message unroutable (%d %s): %s", returned.ReplyCode, returned.ReplyText, returned.RoutingKey)
	case confirmation, ok := <-p.confirms:
		if !ok {
			return fmt.Errorf("messaging: confirm channel closed")
		}
		if !confirmation.Ack {
			return fmt.Errorf("messaging: broker nacked delivery %d", confirmation.DeliveryTag)
		}
		// basic.return precedes basic.ack on the wire; drain a return that arrived
		// just before the confirm so it is not mistaken for the next publish.
		select {
		case returned := <-p.returns:
			return fmt.Errorf("messaging: message unroutable (%d %s): %s", returned.ReplyCode, returned.ReplyText, returned.RoutingKey)
		default:
		}
		return nil
	}
}
