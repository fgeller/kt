package main

import (
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type consumeCmd struct {
	mu sync.Mutex

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
		pom, err := cmd.getPOM(partition)
		if err != nil {
			return 0, err
		}
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
	version     sarama.KafkaVersion
	encodeValue string
	encodeKey   string
	pretty      bool
	group       string
}

func (cmd *consumeCmd) parseArgs(as []string) error {
	args := cmd.parseFlags(as)
	cmd.topic = args.topic
	cmd.tlsCA = args.tlsCA
	cmd.tlsCert = args.tlsCert
	cmd.tlsCertKey = args.tlsCertKey
	cmd.timeout = args.timeout
	cmd.verbose = args.verbose
	cmd.pretty = args.pretty
	cmd.version = args.version
	cmd.group = args.group

	if args.encodeValue != "string" && args.encodeValue != "hex" && args.encodeValue != "base64" {
		return fmt.Errorf(`unsupported encodevalue argument %#v, only string, hex and base64 are supported`, args.encodeValue)
	}
	cmd.encodeValue = args.encodeValue

	if args.encodeKey != "string" && args.encodeKey != "hex" && args.encodeKey != "base64" {
		return fmt.Errorf(`unsupported encodekey argument %#v, only string, hex and base64 are supported`, args.encodeValue)
	}
	cmd.encodeKey = args.encodeKey

	cmd.brokers = strings.Split(args.brokers, ",")
	for i, b := range cmd.brokers {
		if !strings.Contains(b, ":") {
			cmd.brokers[i] = b + ":9092"
		}
	}
	var err error
	cmd.offsets, err = parseOffsets(args.offsets, time.Now())
	if err != nil {
		return err
	}
	return nil
}

func (cmd *consumeCmd) parseFlags(as []string) consumeArgs {
	var args consumeArgs
	flags := flag.NewFlagSet("consume", flag.ContinueOnError)
	flags.StringVar(&args.topic, "topic", "", "Topic to consume (required).")
	flags.StringVar(&args.brokers, "brokers", "localhost:9092", "Comma separated list of brokers. Port defaults to 9092 when omitted.")
	flags.StringVar(&args.tlsCA, "tlsca", "", "Path to the TLS certificate authority file")
	flags.StringVar(&args.tlsCert, "tlscert", "", "Path to the TLS client certificate file")
	flags.StringVar(&args.tlsCertKey, "tlscertkey", "", "Path to the TLS client certificate key file")
	flags.StringVar(&args.offsets, "offsets", "", "Specifies what messages to read by partition and offset range (defaults to all).")
	flags.DurationVar(&args.timeout, "timeout", time.Duration(0), "Timeout after not reading messages (default 0 to disable).")
	flags.BoolVar(&args.verbose, "verbose", false, "More verbose logging to stderr.")
	flags.BoolVar(&args.pretty, "pretty", true, "Control output pretty printing.")
	kafkaVersionFlagVar(flags, &args.version)
	flags.StringVar(&args.encodeValue, "encodevalue", "string", "Present message value as (string|hex|base64), defaults to string.")
	flags.StringVar(&args.encodeKey, "encodekey", "string", "Present message key as (string|hex|base64), defaults to string.")
	flags.StringVar(&args.group, "group", "", "Consumer group to use for marking offsets. kt will mark offsets if this arg is supplied.")

	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of consume:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, consumeDocString)
	}

	err := flags.Parse(as)
	if err == flag.ErrHelp {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}

	if err := setFlagsFromEnv(flags, map[string]string{
		"topic":   "KT_TOPIC",
		"brokers": "KT_BROKERS",
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	return args
}

func (cmd *consumeCmd) run(args []string) {
	if err := cmd.parseArgs(args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		failf(`use "kt consume -help" for more information`)
	}
	if err := cmd.run1(args); err != nil {
		failf("%v", err)
	}
}

func (cmd *consumeCmd) run1(args []string) error {
	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	c, err := cmd.newClient()
	if err != nil {
		return err
	}
	cmd.client = c
	if cmd.group != "" {
		cmd.offsetManager, err = sarama.NewOffsetManagerFromClient(cmd.group, cmd.client)
		if err != nil {
			return fmt.Errorf("cannot create offset manager: %v", err)
		}
	}

	consumer, err := sarama.NewConsumerFromClient(cmd.client)
	if err != nil {
		return fmt.Errorf("cannot create kafka consumer: %v", err)
	}
	cmd.consumer = consumer
	defer logClose("consumer", cmd.consumer)

	partitions, err := cmd.findPartitions()
	if err != nil {
		return err
	}
	defer cmd.closePOMs()

	if err := cmd.consume(partitions); err != nil {
		return err
	}
	return nil
}

func (cmd *consumeCmd) newClient() (sarama.Client, error) {
	cfg := sarama.NewConfig()
	cfg.Version = cmd.version
	usr, err := user.Current()
	var username string
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to read current user name: %v", err)
		username = "anon"
	} else {
		username = usr.Username
	}
	cfg.ClientID = "kt-consume-" + sanitizeUsername(username)
	if cmd.verbose {
		fmt.Fprintf(os.Stderr, "sarama client configuration %#v\n", cfg)
	}
	tlsConfig, err := setupCerts(cmd.tlsCert, cmd.tlsCA, cmd.tlsCertKey)
	if err != nil {
		return nil, fmt.Errorf("cannot set up certificates: %v", err)
	}
	if tlsConfig != nil {
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = tlsConfig
	}

	client, err := sarama.NewClient(cmd.brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("cannot create kafka client: %v", err)
	}
	return client, nil
}

func (cmd *consumeCmd) consume(partitions []int32) error {
	var wg sync.WaitGroup
	out := make(chan printContext)
	go print(out, cmd.pretty)

	wg.Add(len(partitions))
	for _, p := range partitions {
		p := p
		go func() {
			defer wg.Done()
			if err := cmd.consumePartition(out, p); err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}()
	}
	wg.Wait()
	return nil
}

func (cmd *consumeCmd) consumePartition(out chan printContext, partition int32) error {
	var (
		err   error
		pcon  sarama.PartitionConsumer
		start int64
		end   int64
	)

	offsets, ok := cmd.offsets[partition]
	if !ok {
		offsets = cmd.offsets[-1]
	}

	// TODO resolve offsets synchronously before we go into the per-partition
	// consumers.

	if start, err = cmd.resolveOffset(offsets.start, partition); err != nil {
		return fmt.Errorf("failed to read start offset for partition %v: %v", partition, err)
	}

	if end, err = cmd.resolveOffset(offsets.end, partition); err != nil {
		return fmt.Errorf("failed to read end offset for partition %v: %v", partition, err)
	}

	if pcon, err = cmd.consumer.ConsumePartition(cmd.topic, partition, start); err != nil {
		return fmt.Errorf("failed to consume partition %v: %v", partition, err)
	}

	return cmd.partitionLoop(out, pcon, partition, end)
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
	cmd.mu.Lock()
	defer cmd.mu.Unlock()
	for p, pom := range cmd.poms {
		if err := pom.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to close partition offset manager for partition %d: %v", p, err)
		}
	}
}

func (cmd *consumeCmd) getPOM(p int32) (sarama.PartitionOffsetManager, error) {
	cmd.mu.Lock()
	defer cmd.mu.Unlock()
	if cmd.poms == nil {
		cmd.poms = map[int32]sarama.PartitionOffsetManager{}
	}
	pom, ok := cmd.poms[p]
	if ok {
		return pom, nil
	}

	pom, err := cmd.offsetManager.ManagePartition(cmd.topic, p)
	if err != nil {
		return nil, fmt.Errorf("cannot create partition offset manager: %v", err)
	}
	cmd.poms[p] = pom
	return pom, nil
}

func (cmd *consumeCmd) partitionLoop(out chan printContext, pc sarama.PartitionConsumer, p int32, end int64) error {
	defer logClose(fmt.Sprintf("partition consumer %v", p), pc)
	var (
		timer   *time.Timer
		pom     sarama.PartitionOffsetManager
		timeout <-chan time.Time
	)

	if cmd.group != "" {
		var err error
		pom, err = cmd.getPOM(p)
		if err != nil {
			return err
		}
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
			return fmt.Errorf("consuming from partition %v timed out after %s", p, cmd.timeout)
		case err := <-pc.Errors():
			return fmt.Errorf("partition %v consumer encountered error %s", p, err)
		case msg, ok := <-pc.Messages():
			if !ok {
				return fmt.Errorf("unexpected closed messages chan")
			}

			m := newConsumedMessage(msg, cmd.encodeKey, cmd.encodeValue)
			ctx := printContext{output: m, done: make(chan struct{})}
			out <- ctx
			<-ctx.done

			if cmd.group != "" {
				pom.MarkOffset(msg.Offset+1, "")
			}

			if end > 0 && msg.Offset >= end {
				return nil
			}
		}
	}
}

// findPartitions returns all the partitions that need to be consumed.
func (cmd *consumeCmd) findPartitions() ([]int32, error) {
	all, err := cmd.consumer.Partitions(cmd.topic)
	if err != nil {
		return nil, fmt.Errorf("cannot get partitions for topic %q: %v", cmd.topic, err)
	}
	if _, hasDefault := cmd.offsets[-1]; hasDefault {
		return all, nil
	}
	var res []int32
	for _, p := range all {
		if _, ok := cmd.offsets[p]; ok {
			res = append(res, p)
		}
	}
	if len(res) == 0 {
		return nil, fmt.Errorf("found no partitions to consume")
	}
	return res, nil
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
