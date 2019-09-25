package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

const (
	maxOffset    int64 = 1<<63 - 1
	offsetResume int64 = -3
)

type positionRange struct {
	startAnchor, endAnchor anchor
	diff                   anchorDiff
}

func (r positionRange) start() position {
	return position{
		anchor: r.startAnchor,
		diff:   r.diff,
	}
}

func (r positionRange) end() position {
	return position{
		anchor: r.endAnchor,
		diff:   r.diff,
	}
}

// position represents a position within the Kafka stream.
// It holds an anchor (an absolute position specified as a
// time stamp or an offset) and a relative position with
// respect to that (specified as an offset delta or a time duration).
type position struct {
	anchor anchor
	diff   anchorDiff
}

// resolved reports whether the position has been
// fully resolved to an absolute offset.
func (p position) resolved() bool {
	return !p.anchor.isTime && p.anchor.offset >= 0 && !p.diff.isDuration && p.diff.offset == 0
}

// anchor represents an absolute offset in the position stream.
type anchor struct {
	// isTime specifies which anchor field is valid.
	// If it's true, the anchor is specified as a time
	// in time; otherwise it's specified as
	// an offset in offset.
	isTime bool

	// offset holds the anchor as an absolute offset.
	// It can be one of sarama.OffsetOldest, sarama.OffsetNewest
	// or offsetResume to signify a relative starting position.
	// This field is only significant when isTime is false.
	offset int64

	// time holds the anchor as a time.
	// This field is only significant when isTime is true.
	time time.Time
}

func (a0 anchor) eq(a1 anchor) bool {
	if a0.isTime != a1.isTime {
		return false
	}
	if a0.isTime {
		return a0.time.Equal(a1.time)
	}
	return a0.offset == a1.offset
}

// anchorDiff represents an offset from an anchor position.
type anchorDiff struct {
	// isDuration specifies which diff field is valid.
	// If it's true, the difference is specified as a duration
	// in the duration field; otherwise it's specified as
	// an offset in offset.
	isDuration bool

	// offset holds the difference as an offset delta.
	offset int64

	// time holds the difference as a duration.
	duration time.Duration
}

// timeRange holds a time range.
// This represents the precision specified in a timestamp
// (for example, when a time is specified as a date,
// the time range will include the whole of that day).
// TODO is this exclusive or inclusive?
type timeRange struct {
	t0, t1 time.Time
}

type interval struct {
	start position
	end   position
}

// parseOffsets parses a set of partition-offset specifiers in the following
// syntax. The grammar uses the BNF-like syntax defined in https://golang.org/ref/spec.
// Timestamps relative to the current day are resolved using now as the current time.
//
//	offsets = [ partitionInterval { "," partitionInterval } ]
//
//	partitionInterval =
//		partition "=" interval |
//		partition |
//		interval
//
//	partition = "all" | number
//
//	interval = [ position ] [ ":" [ position ] ]
//
//	position =
//		relativePosition |
//		anchorPosition [ relativePosition ]
//
//	anchorPosition = number | "newest" | "oldest" | "resume" | "[" { /^]/ } "]"
//
//	relativePosition = ( "+" | "-" ) (number | duration )
//
//	duration := durationPart { durationPart }
//
//	durationPart =  number [ "." { digit } ] ( "h" | "m" | "s" | "ms" | "ns" )
//
//	number = digit { digit }
//
//	digit = "0"| "1"| "2"| "3"| "4"| "5"| "6"| "7"| "8"| "9"
func parseOffsets(str string, now time.Time) (map[int32]interval, error) {
	result := map[int32]interval{}
	for _, partitionInfo := range strings.Split(str, ",") {
		partitionInfo = strings.TrimSpace(partitionInfo)
		// There's a grammatical ambiguity between a partition
		// number and an interval, because both allow a single
		// decimal number. We work around that by trying an explicit
		// partition first.
		p, err := parsePartition(partitionInfo)
		if err == nil {
			result[p] = interval{
				start: oldestPosition(),
				end:   lastPosition(),
			}
			continue
		}
		intervalStr := partitionInfo
		if i := strings.Index(partitionInfo, "="); i >= 0 {
			// There's an explicitly specified partition.
			p, err = parsePartition(partitionInfo[0:i])
			if err != nil {
				return nil, err
			}
			intervalStr = partitionInfo[i+1:]
		} else {
			// No explicit partition, so implicitly use "all".
			p = -1
		}
		intv, err := parseInterval(intervalStr, now)
		if err != nil {
			return nil, err
		}
		result[p] = intv
	}
	return result, nil
}

func parseInterval(s string, now time.Time) (interval, error) {
	if s == "" {
		// An empty string implies all messages.
		return interval{
			start: oldestPosition(),
			end:   lastPosition(),
		}, nil
	}
	startPos, endStr, err := parsePosition(s, oldestAnchor(), now)
	if err != nil {
		return interval{}, err
	}
	if len(endStr) == 0 {
		// The interval is represented by a single position.

		if startPos.startAnchor.eq(startPos.endAnchor) {
			// The position is precisely specified, so it represents
			// the range from there until the end.
			return interval{
				start: startPos.start(),
				end:   lastPosition(),
			}, nil
		}
		// The position implied a range, so the interval holds that range.
		return interval{
			start: startPos.start(),
			end:   startPos.end(),
		}, nil
	}
	if endStr[0] != ':' {
		return interval{}, fmt.Errorf("invalid interval %q", s)
	}
	endStr = endStr[1:]
	endPos, rest, err := parsePosition(endStr, lastAnchor(), now)
	if err != nil {
		return interval{}, err
	}
	if rest != "" {
		return interval{}, fmt.Errorf("invalid interval %q", s)
	}
	return interval{
		start: startPos.start(),
		end:   endPos.end(),
	}, nil
}

func isDigit(r rune) bool {
	return '0' <= r && r <= '9'
}

func isLower(r rune) bool {
	return 'a' <= r && r <= 'z'
}

// parsePosition parses one half of an interval pair
// and returns that offset and any characters remaining in s.
//
// If s is empty, the given default position will be used.
// Note that a position is always terminated by a colon (the
// interval position divider) or the end of the string.
func parsePosition(s string, defaultAnchor anchor, now time.Time) (positionRange, string, error) {
	var anchorStr string
	switch {
	case s == "":
		// It's empty - we'll get the default position.
	case s[0] == '[':
		// It looks like a timestamp.
		i := strings.Index(s, "]")
		if i == -1 {
			return positionRange{}, "", fmt.Errorf("no closing ] found in %q", s)
		}
		anchorStr, s = s[0:i+1], s[i+1:]
	case isDigit(rune(s[0])):
		// It looks like an absolute offset anchor; find first non-digit following it.
		i := strings.IndexFunc(s, func(r rune) bool { return !isDigit(r) })
		if i > 0 {
			anchorStr, s = s[0:i], s[i:]
		} else {
			anchorStr, s = s, ""
		}
	case isLower(rune(s[0])):
		// It looks like one of the special anchor position names, such as "oldest";
		// find first non-letter following it.
		i := strings.IndexFunc(s, func(r rune) bool { return !isLower(r) })
		if i > 0 {
			anchorStr, s = s[0:i], s[i:]
		} else {
			anchorStr, s = s, ""
		}
	case s[0] == '+':
		// No anchor and a positive relative pos: anchor at the start.
		defaultAnchor = oldestAnchor()
	case s[0] == '-':
		// No anchor and a negative relative pos: anchor at the end.
		defaultAnchor = newestAnchor()
	default:
		return positionRange{}, "", fmt.Errorf("invalid position %q", s)
	}
	var relStr, rest string
	// Look for the termination of the relative part.
	if i := strings.Index(s, ":"); i >= 0 {
		relStr, rest = s[0:i], s[i:]
	} else {
		relStr, rest = s, ""
	}
	a0, a1, err := parseAnchorPos(anchorStr, defaultAnchor, now)
	if err != nil {
		return positionRange{}, "", err
	}
	d, err := parseRelativePosition(relStr)
	if err != nil {
		return positionRange{}, "", err
	}
	if a0.isTime == d.isDuration {
		// We might be able to combine the offset with the diff.
		if d.isDuration {
			a0.time = a0.time.Add(d.duration)
			a1.time = a1.time.Add(d.duration)
			d = anchorDiff{}
		} else if a0.offset >= 0 {
			a0.offset += d.offset
			a1.offset += d.offset
			d = anchorDiff{}
		}
	}
	return positionRange{
		startAnchor: a0,
		endAnchor:   a1,
		diff:        d,
	}, rest, nil
}

// parseAnchorPos parses an anchor position and returns the range
// of possible anchor positions (from a0 to a1).
func parseAnchorPos(s string, defaultAnchor anchor, now time.Time) (a0, a1 anchor, err error) {
	if s == "" {
		return defaultAnchor, defaultAnchor, nil
	}
	n, err := strconv.ParseUint(s, 10, 63)
	if err == nil {
		// It's an explicit numeric offset.
		a := anchor{offset: int64(n)}
		return a, a, nil
	}
	if err := err.(*strconv.NumError); err.Err == strconv.ErrRange {
		return anchor{}, anchor{}, fmt.Errorf("anchor offset %q is too large", s)
	}
	if s[0] == '[' {
		// It's a timestamp.
		// Note: parsePosition has already ensured that the string ends
		// with a ] character.
		t, err := parseTime(s[1:len(s)-1], false, now)
		if err != nil {
			return anchor{}, anchor{}, err
		}
		return anchor{
				isTime: true,
				time:   t.t0,
			}, anchor{
				isTime: true,
				time:   t.t1,
			}, nil
	}
	var a anchor
	switch s {
	case "newest":
		a = newestAnchor()
	case "oldest":
		a = oldestAnchor()
	case "resume":
		a = anchor{offset: offsetResume}
	default:
		return anchor{}, anchor{}, fmt.Errorf("invalid anchor position %q", s)
	}
	return a, a, nil
}

// parseRelativePosition parses a relative position, "-10", "+3", "+1h" or "-3m3s".
//
// The caller has already ensured that s starts with a sign character.
func parseRelativePosition(s string) (anchorDiff, error) {
	if s == "" {
		return anchorDiff{}, nil
	}
	diff, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return anchorDiff{
			offset: diff,
		}, nil
	}
	if err := err.(*strconv.NumError); err.Err == strconv.ErrRange {
		return anchorDiff{}, fmt.Errorf("offset %q is too large", s)
	}
	// It looks like a duration.
	d, err := time.ParseDuration(s)
	if err != nil {
		return anchorDiff{}, fmt.Errorf("invalid relative position %q", s)
	}
	return anchorDiff{
		isDuration: true,
		duration:   d,
	}, nil
}

// parsePartition parses a partition number, or the special
// word "all", meaning all partitions.
func parsePartition(s string) (int32, error) {
	if s == "all" {
		return -1, nil
	}
	p, err := strconv.ParseUint(s, 10, 31)
	if err != nil {
		if err := err.(*strconv.NumError); err.Err == strconv.ErrRange {
			return 0, fmt.Errorf("partition number %q is too large", s)
		}
		return 0, fmt.Errorf("invalid partition number %q", s)
	}
	return int32(p), nil
}

// parseTime parses s in one of a range of possible formats, and returns
// the range of time intervals that it represents.
//
// Any missing information in s will be filled in by using information from now.
// If local is true, times without explicit time zones will be interpreted
// relative to now.Location().
func parseTime(s string, local bool, now time.Time) (timeRange, error) {
	var r timeRange
	var err error
	if r.t0, err = time.Parse(time.RFC3339, s); err == nil {
		r.t1 = r.t0
		// RFC3339 always contains an explicit time zone, so we don't need
		// to convert to local time.
		return r, nil
	} else if r.t0, err = time.Parse("2006-01-02", s); err == nil {
		// A whole day.
		r.t1 = r.t0.AddDate(0, 0, 1)
	} else if r.t0, err = time.Parse("2006-01", s); err == nil {
		// A whole month.
		r.t1 = r.t0.AddDate(0, 1, 0)
	} else if r.t0, err = time.Parse("2006", s); err == nil && r.t0.Year() > 2000 {
		// A whole year.
		r.t1 = r.t0.AddDate(1, 0, 0)
	} else if r.t0, err = time.Parse("15:04", s); err == nil {
		// A minute in the current day. There's an argument that we should choose the closest day
		// that contains the given time (e.g. if the time is 23:30 and the input is 01:20, perhaps
		// we should choose tomorrow morning rather than the morning of the current day).
		r.t0 = time.Date(now.Year(), now.Month(), now.Day(), r.t0.Hour(), r.t0.Minute(), 0, 0, time.UTC)
		r.t1 = r.t0.Add(time.Minute)
	} else if r.t0, err = time.Parse("15:04:05", s); err == nil {
		// An exact moment in the current day.
		r.t0 = time.Date(now.Year(), now.Month(), now.Day(), r.t0.Hour(), r.t0.Minute(), r.t0.Second(), r.t0.Nanosecond(), time.UTC)
		r.t1 = r.t0
	} else if r.t0, err = time.Parse("3pm", s); err == nil {
		// An hour in the current day.
		r.t0 = time.Date(now.Year(), now.Month(), now.Day(), r.t0.Hour(), 0, 0, 0, time.UTC)
		r.t1 = r.t0.Add(time.Hour)
	}
	if local {
		r.t0 = timeWithLocation(r.t0, now.Location())
		r.t1 = timeWithLocation(r.t1, now.Location())
	}
	return r, nil
}

func timeWithLocation(t time.Time, loc *time.Location) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), loc)
}

func anchorAtOffset(off int64) anchor {
	return anchor{
		offset: off,
	}
}

func anchorAtTime(t time.Time) anchor {
	return anchor{
		isTime: true,
		time:   t,
	}
}

func oldestAnchor() anchor {
	return anchorAtOffset(sarama.OffsetOldest)
}

func newestAnchor() anchor {
	return anchorAtOffset(sarama.OffsetNewest)
}

func lastAnchor() anchor {
	return anchorAtOffset(maxOffset)
}

func oldestPosition() position {
	return position{
		anchor: oldestAnchor(),
	}
}

func newestPosition() position {
	return position{
		anchor: newestAnchor(),
	}
}

func lastPosition() position {
	return position{
		anchor: lastAnchor(),
	}
}
