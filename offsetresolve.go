package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

// resolveOffsets resolves the given per-partition intervals to absolute intervals;
// it also returns the limit offset for each partition.
func (cmd *consumeCmd) resolveOffsets(ctx context.Context, offsets map[int32]interval) (resolved map[int32]resolvedInterval, limits map[int32]int64, err error) {
	r, err := cmd.newResolver()
	if err != nil {
		return nil, nil, err
	}
	return r.resolveOffsets(ctx, offsets)
}

func (cmd *consumeCmd) newResolver() (*resolver, error) {
	queryer := &offsetQueryer{
		topic:    cmd.topic,
		client:   cmd.client,
		consumer: cmd.consumer,
	}
	return &resolver{
		truncate:      !cmd.follow,
		runQuery:      queryer.runQuery,
		topic:         cmd.topic,
		allPartitions: cmd.allPartitions,
		info: offsetInfo{
			offsets: make(map[int32]map[offsetRequest]int64),
			times:   make(map[int32]map[int64]time.Time),
		},
	}, nil
}

// resolver is responsible for resolving a set of offsets, as parsed
// by parseOffsets, into absolute integer offsets.
type resolver struct {
	// runQuery is called to make the query to kafka for all items
	// in q, putting the results into info.
	runQuery func(ctx context.Context, q *offsetQuery, info *offsetInfo) error

	// When truncate is true, resolved intervals will not extend beyond the
	// end of their partition.
	truncate bool

	// topic holds the topic being consumed.
	topic string

	// allPartitions holds all the partition ids in the Kafka instance.
	allPartitions []int32

	// info records offset information as it's found.
	info offsetInfo
}

type resolvedInterval struct {
	start, end int64
}

type offsetRequest struct {
	// timeOrOff holds the number of milliseconds since Jan 1st 1970 of the
	// offset to request, or one of sarama.OldestOffset or sarama.NewestOffset
	// if it's not a time-based request.
	//
	// Note that this is in the same form expected by the ListOffset Kakfa API.
	timeOrOff int64
}

// offsetQuery represents a set of information to be asked as bulk requests
// to the Kafka API.
type offsetQuery struct {
	timeQuery   map[int32]map[int64]bool
	offsetQuery map[int32]map[offsetRequest]bool
}

func (r *resolver) resolveOffsets(ctx context.Context, offsets map[int32]interval) (resolved map[int32]resolvedInterval, limits map[int32]int64, err error) {
	allOffsets := make(map[int32]*interval)
	for p, intv := range offsets {
		intv := intv
		allOffsets[p] = &intv
		if p != -1 && !r.partitionExists(p) {
			return nil, nil, fmt.Errorf("partition %v does not exist", p)
		}
	}
	resolved = make(map[int32]resolvedInterval)
	limits = make(map[int32]int64)
	// Some offsets can't be resolved in one query (for example, a
	// offset like "latest-1h" will first need to resolve "latest"
	// to the latest offset for the partition, then find the
	// timestamp of the latest message in that partition, then find
	// the offset of that timestamp less one house). So we keep on
	// running queries until there are no more left, moving items
	// out of allOffsets and into resolved when they're fully
	// resolved.
	count := 0
	for len(allOffsets) > 0 {
		count++
		if count > 20 {
			panic("probable infinite loop resolving offsets")
		}
		// We want to minimise the number of queries we make to
		// the server, so instead of querying each position
		// directly, we build up all the query requirements in
		// q, then call runOffsetQuery which will build bulk API
		// calls as needed, and fill in entries inside info,
		// which can then be used to satisfy subsequent
		// information requirements in resolvePosition.
		q := &offsetQuery{
			timeQuery:   make(map[int32]map[int64]bool),
			offsetQuery: make(map[int32]map[offsetRequest]bool),
		}
		r.expandAllIntervalsSpec(allOffsets, q)
		for p, intv := range allOffsets {
			if p == -1 {
				continue
			}
			limit, haveLimit := resolvePosition(p, newestPosition(), &r.info, q)
			if haveLimit {
				limits[p] = limit.anchor.offset
			}
			partitionMax, ok0 := lastPosition(), true
			if r.truncate {
				// Find out where the partition ends so we can constrain the interval.
				partitionMax, ok0 = limit, haveLimit
			}
			start, ok1 := resolvePosition(p, intv.start, &r.info, q)
			end, ok2 := resolvePosition(p, intv.end, &r.info, q)
			intv.start, intv.end = start, end
			if ok0 && ok1 && ok2 {
				// The interval has been fully resolved,
				// so remove it from allOffsets and add
				// it to resolved.
				delete(allOffsets, p)
				if start.resolved() && end.resolved() {
					resolved[p] = resolvedInterval{
						start: min(start.anchor.offset, partitionMax.anchor.offset),
						end:   min(end.anchor.offset, partitionMax.anchor.offset),
					}
				}
			}
		}
		if err := r.runQuery(ctx, q, &r.info); err != nil {
			return nil, nil, err
		}
	}
	return resolved, limits, nil
}

// expandAllIntervalsSpec expands the "all partitions" entry in offsets if present,
// creating entries for all partitions not explicitly specified in offsets.
//
// If there isn't enough information in q to do that, it leaves
// the offsets map unchanged.
func (r *resolver) expandAllIntervalsSpec(offsets map[int32]*interval, q *offsetQuery) {
	intv, ok := offsets[-1]
	if !ok {
		// No "all partitions" entry.
		return
	}
	// updateTimestamp updates the "summary" timestamp *t according to p.
	// It only affects t if p has either an "oldest" or "newest" anchor with
	// a duration-based difference which implies that we need to find either the
	// oldest or newest timestamp across all partitions so that the
	// interval end time is the same across all partitions.
	updateTimestamp := func(t *time.Time, partition int32, p position) bool {
		if p.anchor.offset >= 0 || !p.diff.isDuration {
			// We can work out the "all" offset for each partition independently.
			return true
		}
		off, ok := r.info.getOffset(partition, symbolicOffsetRequest(p.anchor.offset), q)
		if !ok {
			return false
		}
		t1, ok := r.info.getTime(partition, off, q)
		if !ok {
			return false
		}
		if t1.IsZero() {
			// No timestamp available (probably because the partition is empty)
			return true
		}
		switch p.anchor.offset {
		case sarama.OffsetOldest:
			if t1.Before(*t) {
				*t = t1
			}
		case sarama.OffsetNewest:
			if (*t).IsZero() || t1.After(*t) {
				*t = t1
			}
		}
		return true
	}
	var startTime, endTime time.Time
	gotAll := true
	for _, partition := range r.allPartitions {
		if _, ok := offsets[partition]; ok {
			// The partition was explicitly specified, so it's independent.
			continue
		}
		gotAll = updateTimestamp(&startTime, partition, intv.start) && gotAll
		gotAll = updateTimestamp(&endTime, partition, intv.end) && gotAll
	}
	if !gotAll {
		return
	}
	// We've got enough information to fill out the offsets we need.
	// First update the "all partitions" interval to be the correct time if needed.
	// Note that if startTime or endTime are zero (because all partitions
	// are empty for example), we'll leave the interval to be resolved later,
	// which should work out fine in the end.
	if !startTime.IsZero() {
		intv.start.anchor.isTime = true
		intv.start.anchor.time = startTime.Add(intv.start.diff.duration)
		intv.start.diff = anchorDiff{}
	}
	if !endTime.IsZero() {
		intv.end.anchor.isTime = true
		intv.end.anchor.time = endTime.Add(intv.end.diff.duration)
		intv.end.diff = anchorDiff{}
	}
	delete(offsets, -1)
	for _, partition := range r.allPartitions {
		if _, ok := offsets[partition]; ok {
			continue
		}
		intv := *intv
		offsets[partition] = &intv
	}
}

func (r *resolver) partitionExists(p int32) bool {
	for _, ap := range r.allPartitions {
		if ap == p {
			return true
		}
	}
	return false
}

// resolvePosition tries to resolve p in the given partition from information provided in info.
// It reports whether the position has been resolved or cannot be resolved.
//
// If it returns false, it will have added at least one thing to be queried to q in
// order to proceed with the resolution.
//
// The returned position is always valid, and may contain updated information.
func resolvePosition(partition int32, p position, info *offsetInfo, q *offsetQuery) (_pos position, _ok bool) {
	if !p.anchor.isTime && p.anchor.offset >= 0 && !p.diff.isDuration && p.diff.offset == 0 {
		return p, true
	}
	if p.anchor.isTime {
		if p.diff.isDuration {
			panic("position has time anchor and duration diff")
		}
		off, ok := info.getOffset(partition, timeOffsetRequest(p.anchor.time), q)
		if !ok {
			return p, false
		}
		p.anchor.isTime = false
		p.anchor.offset = off
		if off >= 0 {
			p.anchor.offset += p.diff.offset
			p.diff = anchorDiff{}
			return p, true
		}
		// The time has resolved to a symbolic offset, which happens
		// when the time query is beyond the end of the topic.
		// Let the offset resolving logic below deal with that.
	}
	if p.anchor.offset < 0 {
		off, ok := info.getOffset(partition, symbolicOffsetRequest(p.anchor.offset), q)
		if !ok {
			return p, false
		}
		p.anchor.offset = off
	}
	if !p.diff.isDuration {
		p.anchor.offset += p.diff.offset
		p.diff = anchorDiff{}
		return p, true
	}
	// It's a non-symbolic offset anchor with a duration diff.
	// Find the time for the offset.
	t, ok := info.getTime(partition, p.anchor.offset, q)
	if !ok {
		return p, false
	}
	if t.IsZero() {
		// No timestamp is available, so we can't resolve the position properly,
		// so just return to unresolved position.
		return p, true
	}
	// Then get the offset for the anchor time plus the anchor diff duration.
	off, ok := info.getOffset(partition, timeOffsetRequest(t.Add(p.diff.duration)), q)
	if !ok {
		return p, false
	}
	p.anchor.offset = off
	p.diff = anchorDiff{}
	if off >= 0 {
		return p, true
	}
	// The time query has resulted in a symbolic offset, so we need to
	// find the absolute offset for it.
	off1, ok := info.getOffset(partition, symbolicOffsetRequest(off), q)
	if !ok {
		return p, false
	}
	p.anchor.offset = off1
	return p, true
}

type offsetInfo struct {
	// offsets maps from partition to offset request to offset.
	// This holds results from both time-based queries and symbolic
	// offset queries (as determined by the offset request itself).
	//
	// Note that the resulting offset value may itself by symbolic,
	// as a request for a time beyond the last available time
	// may result in a reference to the last available offset.
	offsets map[int32]map[offsetRequest]int64

	// offsetTimes maps from partition to offset to the timestamp for that offset in that partition.
	times map[int32]map[int64]time.Time
}

// getOffset returns the offset for a given time or symbolic offset.
func (info *offsetInfo) getOffset(p int32, req offsetRequest, q *offsetQuery) (int64, bool) {
	if off, ok := info.offsets[p][req]; ok {
		return off, true
	}
	if q.offsetQuery[p] == nil {
		q.offsetQuery[p] = make(map[offsetRequest]bool)
	}
	q.offsetQuery[p][req] = true
	return 0, false
}

func (info *offsetInfo) setOffset(p int32, req offsetRequest, off int64) {
	if info.offsets[p] == nil {
		info.offsets[p] = make(map[offsetRequest]int64)
	}
	info.offsets[p][req] = off
}

func (info *offsetInfo) getTime(p int32, off int64, q *offsetQuery) (time.Time, bool) {
	if off < 0 {
		panic("getTime called with symbolic offset")
	}
	// So that we can avoid blocking if the offset is beyond the end of the
	// partition, the queryer needs the offset of the last message in the partition,
	// so ensure that's available.
	// If the offset is beyond the end, we won't be able to get a time
	// for it, but we can't return an error here, so leave it to the offsetQueryer
	// to do that for us.
	_, ok := info.getOffset(p, symbolicOffsetRequest(sarama.OffsetNewest), q)
	if !ok {
		return time.Time{}, false
	}
	t, ok := info.times[p][off]
	if ok {
		return t, true
	}
	if q.timeQuery[p] == nil {
		q.timeQuery[p] = make(map[int64]bool)
	}
	q.timeQuery[p][off] = true
	return time.Time{}, false
}

func (info *offsetInfo) setTime(p int32, off int64, t time.Time) {
	if info.times[p] == nil {
		info.times[p] = make(map[int64]time.Time)
	}
	info.times[p][off] = t
}

func timeOffsetRequest(t time.Time) offsetRequest {
	return offsetRequest{
		timeOrOff: unixMilliseconds(t),
	}
}

func symbolicOffsetRequest(off int64) offsetRequest {
	if off >= 0 {
		panic("symbolicOffsetRequest called with non-symbolic offset")
	}
	return offsetRequest{
		timeOrOff: off,
	}
}

func unixMilliseconds(t time.Time) int64 {
	ns := time.Duration(t.UnixNano())
	return int64(ns / time.Millisecond)
}

func fromUnixMilliseconds(ts int64) time.Time {
	if ts <= 0 {
		return time.Time{}
	}
	return time.Unix(ts/1000, (ts%1000)*1e6)
}
