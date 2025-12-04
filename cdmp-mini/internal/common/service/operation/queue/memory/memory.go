package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/maxiaolu1981/cretem/cdmp-mini/internal/common/service/operation"
)

// Coordinator is an in-memory QueueCoordinator implementation used for
// development and testing. It provides simple FIFO behaviour with optional
// delay support but should not be used in production.
type Coordinator struct {
	mu       sync.Mutex
	queue    []*operation.QueueItem
	tickets  map[string]*ticketState
	inFlight map[string]*operation.QueueItem
}

type ticketState struct {
	ticket operation.QueueTicket
}

// NewCoordinator constructs an empty in-memory coordinator.
func NewCoordinator() *Coordinator {
	return &Coordinator{
		queue:    make([]*operation.QueueItem, 0, 32),
		tickets:  make(map[string]*ticketState),
		inFlight: make(map[string]*operation.QueueItem),
	}
}

// Enqueue implements operation.QueueCoordinator.
func (c *Coordinator) Enqueue(_ context.Context, env *operation.OperationEnvelope) (*operation.QueueTicket, error) {
	if env == nil {
		return nil, fmt.Errorf("operation envelope is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	item := &operation.QueueItem{
		Envelope:    env,
		Attempts:    0,
		AvailableAt: time.Now(),
	}
	c.queue = append(c.queue, item)

	ticketID := env.ID
	if ticketID == "" {
		ticketID = env.TraceID
	}
	if ticketID == "" {
		ticketID = fmt.Sprintf("ticket-%d", time.Now().UnixNano())
	}

	ticket := operation.QueueTicket{
		TicketID:      ticketID,
		OperationID:   env.ID,
		QueuePosition: int64(len(c.queue) - 1),
		IssuedAt:      time.Now(),
	}
	c.tickets[ticketID] = &ticketState{ticket: ticket}

	return copyTicket(ticket), nil
}

// Poll returns the current queue status for the provided ticket ID.
func (c *Coordinator) Poll(_ context.Context, ticketID string) (*operation.QueueStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ts, ok := c.tickets[ticketID]
	if !ok {
		return nil, fmt.Errorf("ticket %s not found", ticketID)
	}
	operationID := ts.ticket.OperationID

	position := int64(-1)
	for idx, item := range c.queue {
		if item == nil || item.Envelope == nil {
			continue
		}
		if item.Envelope.ID == operationID {
			position = int64(idx)
			break
		}
	}

	state := operation.StateQueued
	if _, inFlight := c.inFlight[operationID]; inFlight {
		position = 0
		state = operation.StateExecuting
	}

	return &operation.QueueStatus{
		OperationID: operationID,
		State:       state,
		Position:    position,
	}, nil
}

// Cancel removes an operation from the queue if it has not been executed yet.
func (c *Coordinator) Cancel(_ context.Context, ticketID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	ts, ok := c.tickets[ticketID]
	if !ok {
		return fmt.Errorf("ticket %s not found", ticketID)
	}
	operationID := ts.ticket.OperationID

	for idx, item := range c.queue {
		if item != nil && item.Envelope != nil && item.Envelope.ID == operationID {
			c.queue = append(c.queue[:idx], c.queue[idx+1:]...)
			delete(c.tickets, ticketID)
			return nil
		}
	}

	delete(c.inFlight, operationID)
	delete(c.tickets, ticketID)
	return nil
}

// Dequeue pops the next available item. If the queue is empty, ErrQueueEmpty is returned.
func (c *Coordinator) Dequeue(_ context.Context) (*operation.QueueItem, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for idx, item := range c.queue {
		if item == nil || item.Envelope == nil {
			continue
		}
		if item.AvailableAt.After(now) {
			continue
		}

		c.queue = append(c.queue[:idx], c.queue[idx+1:]...)
		c.inFlight[item.Envelope.ID] = item
		return item, nil
	}

	return nil, operation.ErrQueueEmpty
}

// Ack marks the operation as processed and removes it from the in-flight map.
func (c *Coordinator) Ack(_ context.Context, operationID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.inFlight, operationID)
	return nil
}

// Requeue places the item back on the queue after the specified delay.
func (c *Coordinator) Requeue(_ context.Context, item *operation.QueueItem, delay time.Duration) error {
	if item == nil {
		return fmt.Errorf("queue item is nil")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	item.AvailableAt = time.Now().Add(delay)
	c.queue = append(c.queue, item)
	return nil
}

func copyTicket(ticket operation.QueueTicket) *operation.QueueTicket {
	cpy := ticket
	return &cpy
}

var _ operation.QueueCoordinator = (*Coordinator)(nil)
