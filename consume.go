package main

import (
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type consumeCmd struct {
	sync.Mutex

	topic       string
	brokers     []string
	tlsCA       string
	tlsCert     string
	tlsCertKey  string
	offsets     map[int32]interval
	timeout     time.Duration
	verbose     bool
	version     sarama.KafkaVersion
	encodeValue string
	encodeKey   string
	pretty      bool
	group       string

	client        sarama.Client
	consumer      sarama.Consumer
	offsetManager sarama.OffsetManager
	poms          map[int32]sarama.PartitionOffsetManager
}

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

func (r timeRange) add(d time.Duration) timeRange {
	return timeRange{
		t0: r.t0.Add(d),
		t1: r.t1.Add(d),
	}
}

type interval struct {
	start position
	end   position
}

func (cmd *consumeCmd) resolveOffset(p position, partition int32) (int64, error) {
	if p.anchor.isTime || p.diff.isDuration {
		return 0, fmt.Errorf("time-based positions not yet supported")
	}
	var startOffset int64
	switch p.anchor.offset {
	case sarama.OffsetNewest, sarama.OffsetOldest:
		off, err := cmd.client.GetOffset(cmd.topic, partition, p.anchor.offset)
		if err != nil {
			return 0, err
		}
		if p.anchor.offset == sarama.OffsetNewest {
			// TODO add comment explaining this.
			off--
		}
		startOffset = off
	case offsetResume:
		if cmd.group == "" {
			return 0, fmt.Errorf("cannot resume without -group argument")
		}
		pom := cmd.getPOM(partition)
		startOffset, _ = pom.NextOffset()
	default:
		startOffset = p.anchor.offset
	}
	return startOffset + p.diff.offset, nil
}

type consumeArgs struct {
	topic       string
	brokers     string
	tlsCA       string
	tlsCert     string
	tlsCertKey  string
	timeout     time.Duration
	offsets     string
	verbose     bool
	version     string
	encodeValue string
	encodeKey   string
	pretty      bool
	group       string
}

func (cmd *consumeCmd) failStartup(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	failf("use \"kt consume -help\" for more information")
}

func (cmd *consumeCmd) parseArgs(as []string) {
	var (
		err  error
		args = cmd.parseFlags(as)
	)

	envTopic := os.Getenv("KT_TOPIC")
	if args.topic == "" {
		if envTopic == "" {
			cmd.failStartup("Topic name is required.")
			return
		}
		args.topic = envTopic
	}
	cmd.topic = args.topic
	cmd.tlsCA = args.tlsCA
	cmd.tlsCert = args.tlsCert
	cmd.tlsCertKey = args.tlsCertKey
	cmd.timeout = args.timeout
	cmd.verbose = args.verbose
	cmd.pretty = args.pretty
	cmd.version = kafkaVersion(args.version)
	cmd.group = args.group

	if args.encodeValue != "string" && args.encodeValue != "hex" && args.encodeValue != "base64" {
		cmd.failStartup(fmt.Sprintf(`unsupported encodevalue argument %#v, only string, hex and base64 are supported.`, args.encodeValue))
		return
	}
	cmd.encodeValue = args.encodeValue

	if args.encodeKey != "string" && args.encodeKey != "hex" && args.encodeKey != "base64" {
		cmd.failStartup(fmt.Sprintf(`unsupported encodekey argument %#v, only string, hex and base64 are supported.`, args.encodeValue))
		return
	}
	cmd.encodeKey = args.encodeKey

	envBrokers := os.Getenv("KT_BROKERS")
	if args.brokers == "" {
		if envBrokers != "" {
			args.brokers = envBrokers
		} else {
			args.brokers = "localhost:9092"
		}
	}
	cmd.brokers = strings.Split(args.brokers, ",")
	for i, b := range cmd.brokers {
		if !strings.Contains(b, ":") {
			cmd.brokers[i] = b + ":9092"
		}
	}

	cmd.offsets, err = parseOffsets(args.offsets, time.Now())
	if err != nil {
		cmd.failStartup(fmt.Sprintf("%s", err))
	}
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

func (cmd *consumeCmd) parseFlags(as []string) consumeArgs {
	var args consumeArgs
	flags := flag.NewFlagSet("consume", flag.ContinueOnError)
	flags.StringVar(&args.topic, "topic", "", "Topic to consume (required).")
	flags.StringVar(&args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	flags.StringVar(&args.tlsCA, "tlsca", "", "Path to the TLS certificate authority file")
	flags.StringVar(&args.tlsCert, "tlscert", "", "Path to the TLS client certificate file")
	flags.StringVar(&args.tlsCertKey, "tlscertkey", "", "Path to the TLS client certificate key file")
	flags.StringVar(&args.offsets, "offsets", "", "Specifies what messages to read by partition and offset range (defaults to all).")
	flags.DurationVar(&args.timeout, "timeout", time.Duration(0), "Timeout after not reading messages (default 0 to disable).")
	flags.BoolVar(&args.verbose, "verbose", false, "More verbose logging to stderr.")
	flags.BoolVar(&args.pretty, "pretty", true, "Control output pretty printing.")
	flags.StringVar(&args.version, "version", "", "Kafka protocol version")
	flags.StringVar(&args.encodeValue, "encodevalue", "string", "Present message value as (string|hex|base64), defaults to string.")
	flags.StringVar(&args.encodeKey, "encodekey", "string", "Present message key as (string|hex|base64), defaults to string.")
	flags.StringVar(&args.group, "group", "", "Consumer group to use for marking offsets. kt will mark offsets if this arg is supplied.")

	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of consume:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, consumeDocString)
	}

	err := flags.Parse(as)
	if err != nil && strings.Contains(err.Error(), "flag: help requested") {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}

	return args
}

func (cmd *consumeCmd) setupClient() {
	var (
		err error
		usr *user.User
		cfg = sarama.NewConfig()
	)
	cfg.Version = cmd.version
	if usr, err = user.Current(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-consume-" + sanitizeUsername(usr.Username)
	if cmd.verbose {
		fmt.Fprintf(os.Stderr, "sarama client configuration %#v\n", cfg)
	}
	tlsConfig, err := setupCerts(cmd.tlsCert, cmd.tlsCA, cmd.tlsCertKey)
	if err != nil {
		failf("failed to setup certificates err=%v", err)
	}
	if tlsConfig != nil {
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = tlsConfig
	}

	if cmd.client, err = sarama.NewClient(cmd.brokers, cfg); err != nil {
		failf("failed to create client err=%v", err)
	}
}

func (cmd *consumeCmd) run(args []string) {
	var err error

	cmd.parseArgs(args)

	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	cmd.setupClient()
	cmd.setupOffsetManager()

	if cmd.consumer, err = sarama.NewConsumerFromClient(cmd.client); err != nil {
		failf("failed to create consumer err=%v", err)
	}
	defer logClose("consumer", cmd.consumer)

	partitions := cmd.findPartitions()
	if len(partitions) == 0 {
		failf("Found no partitions to consume")
	}
	defer cmd.closePOMs()

	cmd.consume(partitions)
}

func (cmd *consumeCmd) setupOffsetManager() {
	if cmd.group == "" {
		return
	}

	var err error
	if cmd.offsetManager, err = sarama.NewOffsetManagerFromClient(cmd.group, cmd.client); err != nil {
		failf("failed to create offsetmanager err=%v", err)
	}
}

func (cmd *consumeCmd) consume(partitions []int32) {
	var (
		wg  sync.WaitGroup
		out = make(chan printContext)
	)

	go print(out, cmd.pretty)

	wg.Add(len(partitions))
	for _, p := range partitions {
		go func(p int32) { defer wg.Done(); cmd.consumePartition(out, p) }(p)
	}
	wg.Wait()
}

func (cmd *consumeCmd) consumePartition(out chan printContext, partition int32) {
	var (
		offsets interval
		err     error
		pcon    sarama.PartitionConsumer
		start   int64
		end     int64
		ok      bool
	)

	if offsets, ok = cmd.offsets[partition]; !ok {
		offsets, ok = cmd.offsets[-1]
	}

	if start, err = cmd.resolveOffset(offsets.start, partition); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read start offset for partition %v err=%v\n", partition, err)
		return
	}

	if end, err = cmd.resolveOffset(offsets.end, partition); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read end offset for partition %v err=%v\n", partition, err)
		return
	}

	if pcon, err = cmd.consumer.ConsumePartition(cmd.topic, partition, start); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to consume partition %v err=%v\n", partition, err)
		return
	}

	cmd.partitionLoop(out, pcon, partition, end)
}

type consumedMessage struct {
	Partition int32      `json:"partition"`
	Offset    int64      `json:"offset"`
	Key       *string    `json:"key"`
	Value     *string    `json:"value"`
	Timestamp *time.Time `json:"timestamp,omitempty"`
}

func newConsumedMessage(m *sarama.ConsumerMessage, encodeKey, encodeValue string) consumedMessage {
	result := consumedMessage{
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       encodeBytes(m.Key, encodeKey),
		Value:     encodeBytes(m.Value, encodeValue),
	}

	if !m.Timestamp.IsZero() {
		result.Timestamp = &m.Timestamp
	}

	return result
}

func encodeBytes(data []byte, encoding string) *string {
	if data == nil {
		return nil
	}

	var str string
	switch encoding {
	case "hex":
		str = hex.EncodeToString(data)
	case "base64":
		str = base64.StdEncoding.EncodeToString(data)
	default:
		str = string(data)
	}

	return &str
}

func (cmd *consumeCmd) closePOMs() {
	cmd.Lock()
	for p, pom := range cmd.poms {
		if err := pom.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to close partition offset manager for partition %v err=%v", p, err)
		}
	}
	cmd.Unlock()
}

func (cmd *consumeCmd) getPOM(p int32) sarama.PartitionOffsetManager {
	cmd.Lock()
	if cmd.poms == nil {
		cmd.poms = map[int32]sarama.PartitionOffsetManager{}
	}
	pom, ok := cmd.poms[p]
	if ok {
		cmd.Unlock()
		return pom
	}

	pom, err := cmd.offsetManager.ManagePartition(cmd.topic, p)
	if err != nil {
		cmd.Unlock()
		failf("failed to create partition offset manager err=%v", err)
	}
	cmd.poms[p] = pom
	cmd.Unlock()
	return pom
}

func (cmd *consumeCmd) partitionLoop(out chan printContext, pc sarama.PartitionConsumer, p int32, end int64) {
	defer logClose(fmt.Sprintf("partition consumer %v", p), pc)
	var (
		timer   *time.Timer
		pom     sarama.PartitionOffsetManager
		timeout = make(<-chan time.Time)
	)

	if cmd.group != "" {
		pom = cmd.getPOM(p)
	}

	for {
		if cmd.timeout > 0 {
			if timer != nil {
				timer.Stop()
			}
			timer = time.NewTimer(cmd.timeout)
			timeout = timer.C
		}

		select {
		case <-timeout:
			fmt.Fprintf(os.Stderr, "consuming from partition %v timed out after %s\n", p, cmd.timeout)
			return
		case err := <-pc.Errors():
			fmt.Fprintf(os.Stderr, "partition %v consumer encountered err %s", p, err)
			return
		case msg, ok := <-pc.Messages():
			if !ok {
				fmt.Fprintf(os.Stderr, "unexpected closed messages chan")
				return
			}

			m := newConsumedMessage(msg, cmd.encodeKey, cmd.encodeValue)
			ctx := printContext{output: m, done: make(chan struct{})}
			out <- ctx
			<-ctx.done

			if cmd.group != "" {
				pom.MarkOffset(msg.Offset+1, "")
			}

			if end > 0 && msg.Offset >= end {
				return
			}
		}
	}
}

func (cmd *consumeCmd) findPartitions() []int32 {
	var (
		all []int32
		res []int32
		err error
	)
	if all, err = cmd.consumer.Partitions(cmd.topic); err != nil {
		failf("failed to read partitions for topic %v err=%v", cmd.topic, err)
	}

	if _, hasDefault := cmd.offsets[-1]; hasDefault {
		return all
	}

	for _, p := range all {
		if _, ok := cmd.offsets[p]; ok {
			res = append(res, p)
		}
	}

	return res
}

var consumeDocString = `
The values for -topic and -brokers can also be set via environment variables KT_TOPIC and KT_BROKERS respectively.
The values supplied on the command line win over environment variable values.

Offsets can be specified as a comma-separated list of intervals:

  [[partition=start:end],...]

For example:

	3=100:300,5=43:67

would consume from offset 100 to offset 300 inclusive in partition 3,
and from 43 to 67 in partition 5.

If the second part of an interval is omitted, there is no upper bound to the interval unless an imprecise timestamp is used (see below).

The default is to consume from the oldest offset on every partition for the given topic.

 - partition is the numeric identifier for a partition. You can use "all" to
   specify a default interval for all partitions.

 - start is the included offset or time where consumption should start.

 - end is the included offset or time where consumption should end.

An offset may be specified as:

- an absolute position as a decimal number (for example "400")

- "oldest", meaning the start of the available messages for the partition.

- "newest", meaning the newest available message in the partition.

- "resume" meaning the most recently consumed message for the
   consumer group (can only be used in combination with -group).

- a timestamp enclosed in square brackets (see below).

A timestamp specifies the offset of the next message found
after the specified time. It may be specified as:

- an RFC3339 time, for example "[2019-09-12T14:49:12Z]")
- an ISO8601 date, for example "[2019-09-12]"
- a month, for example "[2019-09]"
- a year, for example "[2019]"
- a minute within the current day, for example "[14:49]"
- a second within the current day, for example "[14:49:12]"
- an hour within the current day, in 12h format, for example "[2pm]".

When a timestamp is specified with seconds precision, a timestamp
represents an exact moment; otherwise it represents the implied
precision of the timestamp (a year represents the whole of that year;
a month represents the whole month, etc).

The UTC time zone will be used unless the -local flag is provided.

When a non-precise timestamp is used as the start of an offset
range, the earliest time in the range is used; when it's used as
the end of a range, the latest time is used. So, for example:

	all=[2019]:[2020]

asks for all partitions from the start of 2019 to the very end of 2020.
If there is only one offset expression with no colon, the implied range
is used, so for example:

	all=[2019-09-12]

will ask for all messages on September 12th 2019.
To ask for all messages starting at a non-precise timestamp,
you can use an empty expression as the second part of the range.
For example:

	all=[2019-09-12]:

will ask for all messages from the start of 2019-09-12 up until the current time.

An absolute offset may also be combined with a relative offset with "+" or "-".
For example:

	all=newest-1000:

will request the latest thousand messages.

	all=oldest+1000:oldest+2000

will request the second thousand messages stored.
The absolute offset may be omitted; it defaults to "newest"
for "-" and "oldest" for "+", so the previous two examples
may be abbreviated to the following:

	all=-1000
	all=+1000:+2000

Relative offsets are based on numeric values and will not take skipped
offsets (e.g. due to compaction) into account.

A relative offset may also be specified as duration, meaning all messages
within that time period. The syntax is that accepted by Go's time.ParseDuration
function, for example:

	3.5s - three and a half seconds
	1s400ms - 1.4 seconds

So, for example:

	all=1000-5m:1000+5m

will ask for all messages in the 10 minute interval around the message
with offset 1000.

Note that if a message with that offset doesn't exist (because of compaction,
for example), the first message found after that offset will be used for the
timestamp.

More examples:

To consume messages from partition 0 between offsets 10 and 20 (inclusive).

  0=10:20

To define an interval for all partitions use -1 as the partition identifier:

  all=2:10

You can also override the offsets for a single partition, in this case 2:

  all=1-10,2=5-10

To consume from multiple partitions:

  0=4:,2=1:10,6

This would consume messages from three partitions:

  - Anything from partition 0 starting at offset 4.
  - Messages between offsets 1 and 10 from partition 2.
  - Anything from partition 6.

To start at the latest offset for each partition:

  all=newest:

Or shorter:

  newest:

To consume the last 10 messages:

  newest-10:

To skip the first 15 messages starting with the oldest offset:

  oldest+10:

In both cases you can omit "newest" and "oldest":

  -10:

and

  +10:

Will achieve the same as the two examples above.

`
