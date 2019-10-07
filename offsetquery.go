package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"golang.org/x/sync/errgroup"
)

// offsetQueryer knows how to send bulk offset queries to Kafka.
type offsetQueryer struct {
	topic    string
	client   sarama.Client
	consumer sarama.Consumer
}

// runQuery runs the given offset query and puts the results into info.
func (r *offsetQueryer) runQuery(ctx context.Context, q *offsetQuery, info *offsetInfo) error {
	egroup, ctx := errgroup.WithContext(ctx)
	egroup.Go(func() error {
		return r.getTimes(ctx, q.timeQuery, info)
	})
	egroup.Go(func() error {
		return r.getOffsets(ctx, q.offsetQuery, info)
	})
	if err := egroup.Wait(); err != nil {
		return err
	}
	return nil
}

func (r *offsetQueryer) getTimes(ctx context.Context, q map[int32]map[int64]bool, info *offsetInfo) error {
	type result struct {
		partition int32
		offset    int64
		time      time.Time
	}
	resultc := make(chan result)
	nrequests := 0
	egroup, ctx := errgroup.WithContext(ctx)
	for p, offsets := range q {
		p := p
		// getOffset should never fail because getTime should have ensured
		// that the newest-offset info is present. If it does, it'll panic before
		// it returns due to assigning to the new query value.
		newestOffset, _ := info.getOffset(p, symbolicOffsetRequest(sarama.OffsetNewest), nil)
		for offset := range offsets {
			offset := offset
			if offset < 0 {
				panic("symbolic offset passed to getTime")
			}
			nrequests++
			egroup.Go(func() error {
				consumeOffset := offset
				if consumeOffset == newestOffset && newestOffset > 0 {
					// We're asking about the time of the newest offset. Although the newest
					// offset is one beyond the last message, we make a special case
					// for this, as finding the time of the last message is common.
					consumeOffset--
				}
				if consumeOffset >= newestOffset {
					// No way to know what the timestamp is, so send a zero timestamp
					// to signify that fact.
					resultc <- result{
						partition: p,
						offset:    offset,
					}
					return nil
				}
				// TODO batch fetch requests.
				mt, err := fetchOneMessageTimestamp(r.client, r.topic, p, consumeOffset)
				if err != nil {
					return err
				}
				resultc <- result{
					partition: p,
					offset:    offset,
					time:      mt,
				}
				return nil
			})
		}
	}
	egroup.Go(func() error {
		for i := 0; i < nrequests; i++ {
			select {
			case result := <-resultc:
				info.setTime(result.partition, result.offset, result.time)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	return egroup.Wait()
}

// getOffsets gets offsets for all the requests in q and stores the results by calling info.setOffset.
func (r *offsetQueryer) getOffsets(ctx context.Context, q map[int32]map[offsetRequest]bool, info *offsetInfo) error {
	type result struct {
		askFor map[int32]offsetRequest
		resp   *sarama.OffsetResponse
		err    error
	}
	// This is unbuffered so if we fail, we'll leave some dangling goroutines that will block trying to
	// write to resultc, but as the whole command is going to exit in that case, we don't care.
	resultc := make(chan result)

	nqueries := 0
	// We can only ask about one partition per request, so keep issuing requests
	// until we have no more left to ask about.
	for len(q) > 0 {
		// We're going to issue a request for each unique broker that manages
		// any of the partitions in the query.
		reqs := make(map[*sarama.Broker]*sarama.OffsetRequest)

		// askFor records the partitions that we're asking about,
		// so we can make sure the broker has responded with
		// the expected information, otherwise there's a risk
		// that a broker with broken behaviour could cause
		// getOffsetsForTimes to fail to fill out the required info
		// in info, which could cause an infinite loop in the calling
		// logic which expects all queries to get an answer.
		askFor := make(map[*sarama.Broker]map[int32]offsetRequest)
		for p, offqs := range q {
			if len(offqs) == 0 {
				// Defensive: this shouldn't happen.
				delete(q, p)
				continue
			}
			var offq offsetRequest
			for offq = range offqs {
				delete(offqs, offq)
				break
			}
			if len(offqs) == 0 {
				delete(q, p)
			}
			b, err := r.client.Leader(r.topic, p)
			if err != nil {
				return err
			}
			req := reqs[b]
			if req == nil {
				req = &sarama.OffsetRequest{
					Version: 1,
				}
				reqs[b] = req
				askFor[b] = make(map[int32]offsetRequest)
			}
			req.AddBlock(r.topic, p, offq.timeOrOff, 1)
			askFor[b][p] = offq
		}
		for b, req := range reqs {
			b, req := b, req
			nqueries++
			go func() {
				// We should pass ctx here but sarama doesn't support context.
				resp, err := b.GetAvailableOffsets(req)
				resultc <- result{
					askFor: askFor[b],
					resp:   resp,
					err:    err,
				}
			}()
		}
	}
	for i := 0; i < nqueries; i++ {
		select {
		case result := <-resultc:
			ps, ok := result.resp.Blocks[r.topic]
			if !ok {
				return fmt.Errorf("topic %q not found in offsets response", r.topic)
			}
			for p, offq := range result.askFor {
				block, ok := ps[p]
				if !ok {
					return fmt.Errorf("no offset found for partition %d", p)
				}
				if block.Err != 0 {
					return fmt.Errorf("cannot get offset for partition %d: %v", p, block.Err)
				}
				if block.Offset < 0 {
					// This happens when the time is beyond the last offset.
					block.Offset = sarama.OffsetNewest // TODO or maxOffset?
				}
				info.setOffset(p, offq, block.Offset)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func fetchOneMessageTimestamp(client sarama.Client, topic string, partition int32, offset int64) (time.Time, error) {
	b, err := client.Leader(topic, partition)
	if err != nil {
		return time.Time{}, err
	}
	req := &sarama.FetchRequest{
		MinBytes:    1,
		MaxWaitTime: 1,
		// TODO better version-picking logic.
		Version: 4,
	}
	req.AddBlock(topic, partition, offset, 1024*1024)
	resp, err := b.Fetch(req)
	if err != nil {
		return time.Time{}, err
	}
	block := resp.Blocks[topic][partition]
	if block == nil {
		return time.Time{}, fmt.Errorf("no block found in response")
	}
	if block.Err != 0 {
		// possible errors:
		// ErrOffsetOutOfRange:
		// ErrUnknownTopicOrPartition, ErrNotLeaderForPartition, ErrLeaderNotAvailable, ErrReplicaNotAvailable:
		return time.Time{}, fmt.Errorf("fetch error: %v", block.Err)
	}
	if len(block.RecordsSet) == 0 {
		return time.Time{}, fmt.Errorf("no record sets found in fetch response")
	}
	for _, records := range block.RecordsSet {
		batch := records.RecordBatch
		if batch == nil {
			return time.Time{}, fmt.Errorf("can't deal with legacy fetch response")
		}
		for _, r := range batch.Records {
			if batch.FirstOffset+r.OffsetDelta >= offset {
				return batch.FirstTimestamp.Add(r.TimestampDelta), nil
			}
		}
	}
	return time.Time{}, fmt.Errorf("no record found")
}
