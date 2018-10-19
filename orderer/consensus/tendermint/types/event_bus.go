package types

import (
	"context"

	cmn "github.com/hyperledger/fabric/orderer/consensus/tendermint/common"
	tmpubsub "github.com/hyperledger/fabric/orderer/consensus/tendermint/pubsub"
	"github.com/hyperledger/fabric/common/flogging"
)

const defaultCapacity = 0

type EventBusSubscriber interface {
	Subscribe(ctx context.Context, subscriber string, query tmpubsub.Query, out chan<- interface{}) error
	Unsubscribe(ctx context.Context, subscriber string, query tmpubsub.Query) error
	UnsubscribeAll(ctx context.Context, subscriber string) error
}

// EventBus is a common bus for all events going through the system. All calls
// are proxied to underlying pubsub server. All events must be published using
// EventBus to ensure correct data types.
type EventBus struct {
	cmn.BaseService
	pubsub *tmpubsub.Server
}

// NewEventBus returns a new event bus.
func NewEventBus() *EventBus {
	return NewEventBusWithBufferCapacity(defaultCapacity)
}

// NewEventBusWithBufferCapacity returns a new event bus with the given buffer capacity.
func NewEventBusWithBufferCapacity(cap int) *EventBus {
	// capacity could be exposed later if needed
	pubsub := tmpubsub.NewServer(tmpubsub.BufferCapacity(cap))
	b := &EventBus{pubsub: pubsub}
	b.BaseService = *cmn.NewBaseService("EventBus", b)
	return b
}

func (b *EventBus) OnStart() error {
	return b.pubsub.OnStart()
}

func (b *EventBus) OnStop() {
	b.pubsub.Stop()
}

func (b *EventBus) Subscribe(ctx context.Context, subscriber string, query tmpubsub.Query, out chan<- interface{}) error {
	return b.pubsub.Subscribe(ctx, subscriber, query, out)
}

func (b *EventBus) Unsubscribe(ctx context.Context, subscriber string, query tmpubsub.Query) error {
	return b.pubsub.Unsubscribe(ctx, subscriber, query)
}

func (b *EventBus) UnsubscribeAll(ctx context.Context, subscriber string) error {
	return b.pubsub.UnsubscribeAll(ctx, subscriber)
}

func (b *EventBus) Publish(eventType string, eventData TMEventData) error {
	// no explicit deadline for publishing events
	ctx := context.Background()
	b.pubsub.PublishWithTags(ctx, eventData, tmpubsub.NewTagMap(map[string]string{EventTypeKey: eventType}))
	return nil
}

//--- block, tx, and vote events

func (b *EventBus) PublishEventNewBlock(event EventDataNewBlock) error {
	return b.Publish(EventNewBlock, event)
}

func (b *EventBus) PublishEventNewBlockHeader(event EventDataNewBlockHeader) error {
	return b.Publish(EventNewBlockHeader, event)
}

func (b *EventBus) PublishEventVote(event EventDataVote) error {
	return b.Publish(EventVote, event)
}

func (b *EventBus) PublishEventProposalHeartbeat(event EventDataProposalHeartbeat) error {
	return b.Publish(EventProposalHeartbeat, event)
}

//--- EventDataRoundState events

func (b *EventBus) PublishEventNewRoundStep(event EventDataRoundState) error {
	return b.Publish(EventNewRoundStep, event)
}

func (b *EventBus) PublishEventTimeoutPropose(event EventDataRoundState) error {
	return b.Publish(EventTimeoutPropose, event)
}

func (b *EventBus) PublishEventTimeoutWait(event EventDataRoundState) error {
	return b.Publish(EventTimeoutWait, event)
}

func (b *EventBus) PublishEventNewRound(event EventDataRoundState) error {
	return b.Publish(EventNewRound, event)
}

func (b *EventBus) PublishEventCompleteProposal(event EventDataRoundState) error {
	return b.Publish(EventCompleteProposal, event)
}

func (b *EventBus) PublishEventPolka(event EventDataRoundState) error {
	return b.Publish(EventPolka, event)
}

func (b *EventBus) PublishEventUnlock(event EventDataRoundState) error {
	return b.Publish(EventUnlock, event)
}

func (b *EventBus) PublishEventRelock(event EventDataRoundState) error {
	return b.Publish(EventRelock, event)
}

func (b *EventBus) PublishEventLock(event EventDataRoundState) error {
	return b.Publish(EventLock, event)
}

func logIfTagExists(tag string, tags map[string]string, logger *flogging.FabricLogger) {
	if value, ok := tags[tag]; ok {
		logger.Error("Found predefined tag (value will be overwritten)", "tag", tag, "value", value)
	}
}
