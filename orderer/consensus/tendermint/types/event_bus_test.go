package types

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tmpubsub "github.com/hyperledger/fabric/orderer/consensus/tendermint/pubsub"
	tmquery "github.com/hyperledger/fabric/orderer/consensus/tendermint/pubsub/query"
)

func TestEventBusPublish(t *testing.T) {
	eventBus := NewEventBus()
	err := eventBus.Start()
	require.NoError(t, err)
	defer eventBus.Stop()

	eventsCh := make(chan interface{})
	err = eventBus.Subscribe(context.Background(), "test", tmquery.Empty{}, eventsCh)
	require.NoError(t, err)

	const numEventsExpected = 14
	done := make(chan struct{})
	go func() {
		numEvents := 0
		for range eventsCh {
			numEvents++
			if numEvents >= numEventsExpected {
				close(done)
			}
		}
	}()

	err = eventBus.Publish(EventNewBlockHeader, EventDataNewBlockHeader{})
	require.NoError(t, err)
	err = eventBus.PublishEventNewBlock(EventDataNewBlock{})
	require.NoError(t, err)
	err = eventBus.PublishEventNewBlockHeader(EventDataNewBlockHeader{})
	require.NoError(t, err)
	err = eventBus.PublishEventVote(EventDataVote{})
	require.NoError(t, err)
	err = eventBus.PublishEventProposalHeartbeat(EventDataProposalHeartbeat{})
	require.NoError(t, err)
	err = eventBus.PublishEventNewRoundStep(EventDataRoundState{})
	require.NoError(t, err)
	err = eventBus.PublishEventTimeoutPropose(EventDataRoundState{})
	require.NoError(t, err)
	err = eventBus.PublishEventTimeoutWait(EventDataRoundState{})
	require.NoError(t, err)
	err = eventBus.PublishEventNewRound(EventDataRoundState{})
	require.NoError(t, err)
	err = eventBus.PublishEventCompleteProposal(EventDataRoundState{})
	require.NoError(t, err)
	err = eventBus.PublishEventPolka(EventDataRoundState{})
	require.NoError(t, err)
	err = eventBus.PublishEventUnlock(EventDataRoundState{})
	require.NoError(t, err)
	err = eventBus.PublishEventRelock(EventDataRoundState{})
	require.NoError(t, err)
	err = eventBus.PublishEventLock(EventDataRoundState{})
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatalf("expected to receive %d events after 1 sec.", numEventsExpected)
	}
}

func BenchmarkEventBus(b *testing.B) {
	benchmarks := []struct {
		name        string
		numClients  int
		randQueries bool
		randEvents  bool
	}{
		{"10Clients1Query1Event", 10, false, false},
		{"100Clients", 100, false, false},
		{"1000Clients", 1000, false, false},

		{"10ClientsRandQueries1Event", 10, true, false},
		{"100Clients", 100, true, false},
		{"1000Clients", 1000, true, false},

		{"10ClientsRandQueriesRandEvents", 10, true, true},
		{"100Clients", 100, true, true},
		{"1000Clients", 1000, true, true},

		{"10Clients1QueryRandEvents", 10, false, true},
		{"100Clients", 100, false, true},
		{"1000Clients", 1000, false, true},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			benchmarkEventBus(bm.numClients, bm.randQueries, bm.randEvents, b)
		})
	}
}

func benchmarkEventBus(numClients int, randQueries bool, randEvents bool, b *testing.B) {
	// for random* functions
	rand.Seed(time.Now().Unix())

	eventBus := NewEventBusWithBufferCapacity(0) // set buffer capacity to 0 so we are not testing cache
	eventBus.Start()
	defer eventBus.Stop()

	ctx := context.Background()
	q := EventQueryNewBlock

	for i := 0; i < numClients; i++ {
		ch := make(chan interface{})
		go func() {
			for range ch {
			}
		}()
		if randQueries {
			q = randQuery()
		}
		eventBus.Subscribe(ctx, fmt.Sprintf("client-%d", i), q, ch)
	}

	eventType := EventNewBlock

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if randEvents {
			eventType = randEvent()
		}

		eventBus.Publish(eventType, EventDataString("Gamora"))
	}
}

var events = []string{
	EventNewBlock,
	EventNewBlockHeader,
	EventNewRound,
	EventNewRoundStep,
	EventTimeoutPropose,
	EventCompleteProposal,
	EventPolka,
	EventUnlock,
	EventLock,
	EventRelock,
	EventTimeoutWait,
	EventVote}

func randEvent() string {
	return events[rand.Intn(len(events))]
}

var queries = []tmpubsub.Query{
	EventQueryNewBlock,
	EventQueryNewBlockHeader,
	EventQueryNewRound,
	EventQueryNewRoundStep,
	EventQueryTimeoutPropose,
	EventQueryCompleteProposal,
	EventQueryPolka,
	EventQueryUnlock,
	EventQueryLock,
	EventQueryRelock,
	EventQueryTimeoutWait,
	EventQueryVote}

func randQuery() tmpubsub.Query {
	return queries[rand.Intn(len(queries))]
}
