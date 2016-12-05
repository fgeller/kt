package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/user"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type consume struct {
	topic   string
	brokers []string
	offsets map[int32]interval
	timeout time.Duration
	verbose bool
	version sarama.KafkaVersion

	client   sarama.Client
	consumer sarama.Consumer

	closer chan struct{}
}

type offset struct {
	relative bool
	start    int64
	diff     int64
}

func (c *consume) resolveOffset(o offset, partition int32) (int64, error) {
	if !o.relative {
		return o.start, nil
	}

	var (
		res int64
		err error
	)

	if o.start == sarama.OffsetNewest || o.start == sarama.OffsetOldest {
		if res, err = c.client.GetOffset(c.topic, partition, o.start); err != nil {
			return 0, err
		}

		if o.start == sarama.OffsetNewest {
			res = res - 1
		}

		return res + o.diff, nil
	}

	return o.start + o.diff, nil
}

type interval struct {
	start offset
	end   offset
}

type consumeConfig struct {
	topic   string
	brokers []string
	offsets map[int32]interval
	timeout time.Duration
	verbose bool
	version sarama.KafkaVersion
}

type consumeArgs struct {
	topic   string
	brokers string
	timeout time.Duration
	offsets string
	verbose bool
	version string
}

func parseOffset(str string) (offset, error) {
	result := offset{}
	re := regexp.MustCompile("(oldest|newest)?(-|\\+)?(\\d+)?")
	matches := re.FindAllStringSubmatch(str, -1)

	if len(matches) == 0 || len(matches[0]) < 4 {
		return result, fmt.Errorf("Could not parse offset [%v]", str)
	}

	startStr := matches[0][1]
	qualifierStr := matches[0][2]
	intStr := matches[0][3]

	var err error
	if result.start, err = strconv.ParseInt(intStr, 10, 64); err != nil && len(intStr) > 0 {
		return result, fmt.Errorf("Invalid offset [%v]", str)
	}

	if len(qualifierStr) > 0 {
		result.relative = true
		result.diff = result.start
		result.start = sarama.OffsetOldest
		if qualifierStr == "-" {
			result.start = sarama.OffsetNewest
			result.diff = -result.diff
		}
	}

	switch startStr {
	case "newest":
		result.relative = true
		result.start = sarama.OffsetNewest
	case "oldest":
		result.relative = true
		result.start = sarama.OffsetOldest
	}

	return result, nil
}

func parseOffsets(str string) (map[int32]interval, error) {
	defaultInterval := interval{
		start: offset{relative: true, start: sarama.OffsetOldest},
		end:   offset{start: 1<<63 - 1},
	}

	if len(str) == 0 {
		return map[int32]interval{-1: defaultInterval}, nil
	}

	result := map[int32]interval{}
	for _, partitionInfo := range strings.Split(str, ",") {
		re := regexp.MustCompile("(all|\\d+)?=?([^:]+)?:?(.+)?")
		matches := re.FindAllStringSubmatch(strings.TrimSpace(partitionInfo), -1)
		if len(matches) != 1 || len(matches[0]) < 3 {
			return result, fmt.Errorf("Invalid partition info [%v]", partitionInfo)
		}

		var partition int32
		start := defaultInterval.start
		end := defaultInterval.end
		partitionMatches := matches[0]

		// partition
		partitionStr := partitionMatches[1]
		if partitionStr == "all" || len(partitionStr) == 0 {
			partition = -1
		} else {
			i, err := strconv.Atoi(partitionStr)
			if err != nil {
				return result, fmt.Errorf("Invalid partition [%v]", partitionStr)
			}
			partition = int32(i)
		}

		// start
		if len(partitionMatches) > 2 && len(strings.TrimSpace(partitionMatches[2])) > 0 {
			startStr := strings.TrimSpace(partitionMatches[2])
			o, err := parseOffset(startStr)
			if err == nil {
				start = o
			}
		}

		// end
		if len(partitionMatches) > 3 && len(strings.TrimSpace(partitionMatches[3])) > 0 {
			endStr := strings.TrimSpace(partitionMatches[3])
			o, err := parseOffset(endStr)
			if err == nil {
				end = o
			}
		}

		result[partition] = interval{start, end}
	}

	return result, nil
}

func failStartup(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	fmt.Fprintln(os.Stderr, "Use \"kt consume -help\" for more information.")
	os.Exit(1)
}

func (c *consume) parseArgs(as []string) {
	var (
		err  error
		args = c.read(as)
	)

	envTopic := os.Getenv("KT_TOPIC")
	if args.topic == "" {
		if envTopic == "" {
			failStartup("Topic name is required.")
		}
		args.topic = envTopic
	}
	c.topic = args.topic
	c.verbose = args.verbose
	c.version = kafkaVersion(args.version)

	envBrokers := os.Getenv("KT_BROKERS")
	if args.brokers == "" {
		if envBrokers != "" {
			args.brokers = envBrokers
		} else {
			args.brokers = "localhost:9092"
		}
	}
	c.brokers = strings.Split(args.brokers, ",")
	for i, b := range c.brokers {
		if !strings.Contains(b, ":") {
			c.brokers[i] = b + ":9092"
		}
	}

	c.offsets, err = parseOffsets(args.offsets)
	if err != nil {
		failStartup(fmt.Sprintf("%s", err))
	}
}

func (c *consume) read(as []string) consumeArgs {
	var args consumeArgs
	flags := flag.NewFlagSet("consume", flag.ExitOnError)
	flags.StringVar(&args.topic, "topic", "", "Topic to consume (required).")
	flags.StringVar(&args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	flags.StringVar(&args.offsets, "offsets", "", "Specifies what messages to read by partition and offset range (defaults to all).")
	flags.DurationVar(&args.timeout, "timeout", time.Duration(0), "Timeout after not reading messages (default 0 to disable).")
	flags.BoolVar(&args.verbose, "verbose", false, "More verbose logging to stderr.")
	flags.StringVar(&args.version, "version", "", "Kafka protocol version")

	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of consume:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, consumeDocString)

		os.Exit(2)
	}

	flags.Parse(as)
	return args
}

func (c *consume) setupClient() {
	var (
		err error
		usr *user.User
		cfg = sarama.NewConfig()
	)
	cfg.Version = c.version
	if usr, err = user.Current(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-consume-" + usr.Username
	if c.verbose {
		fmt.Fprintf(os.Stderr, "sarama client configuration %#v\n", cfg)
	}

	if c.client, err = sarama.NewClient(c.brokers, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client err=%v\n", err)
		os.Exit(1)
	}
}

func (c *consume) run(closer chan struct{}) {
	var err error
	c.closer = closer

	if c.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	c.setupClient()

	if c.consumer, err = sarama.NewConsumerFromClient(c.client); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer err=%v\n", err)
		os.Exit(1)
	}
	defer logClose("consumer", c.consumer)

	partitions := c.findPartitions()
	if len(partitions) == 0 {
		fmt.Fprintf(os.Stderr, "Found no partitions to consume.\n")
		os.Exit(1)
	}

	c.consume(partitions)
}

func print(out <-chan printContext) {
	for {
		ctx := <-out
		fmt.Println(ctx.line)
		close(ctx.done)
	}
}

func (c *consume) consume(partitions []int32) {
	var (
		wg  sync.WaitGroup
		out = make(chan printContext)
	)

	go print(out)

	wg.Add(len(partitions))
	for _, p := range partitions {
		go func(p int32) { defer wg.Done(); c.consumePartition(out, p) }(p)
	}
	wg.Wait()
}

func (c *consume) consumePartition(out chan printContext, partition int32) {
	var (
		offsets interval
		err     error
		pcon    sarama.PartitionConsumer
		start   int64
		end     int64
		ok      bool
	)

	if offsets, ok = c.offsets[partition]; !ok {
		offsets, ok = c.offsets[-1]
	}

	if start, err = c.resolveOffset(offsets.start, partition); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read start offset for partition %v err=%v\n", partition, err)
		return
	}

	if end, err = c.resolveOffset(offsets.end, partition); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read end offset for partition %v err=%v\n", partition, err)
		return
	}

	if pcon, err = c.consumer.ConsumePartition(c.topic, partition, start); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to consume partition %v err=%v\n", partition, err)
		return
	}

	c.partitionLoop(out, pcon, partition, end)
}

type consumedMessage struct {
	Partition int32   `json:"partition"`
	Offset    int64   `json:"offset"`
	Key       *string `json:"key"`
	Value     *string `json:"value"`
}

func logClose(name string, c io.Closer) {
	if err := c.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to close %#v err=%v", name, err)
	}
}

type printContext struct {
	line string
	done chan struct{}
}

func (c *consume) partitionLoop(out chan printContext, pc sarama.PartitionConsumer, p int32, end int64) {
	defer logClose(fmt.Sprintf("partition consumer %v", p), pc)

	for {
		timeout := make(<-chan time.Time)
		if c.timeout > 0 {
			timeout = time.After(c.timeout)
		}

		select {
		case <-timeout:
			fmt.Fprintf(os.Stderr, "consuming from partition %v timed out after %s.", p, c.timeout)
			return
		case <-c.closer:
			fmt.Fprintf(os.Stderr, "shuttin down partition consumer for partition %v\n", p)
			return
		case msg, ok := <-pc.Messages():
			var (
				buf []byte
				err error

				k = string(msg.Key)
				v = string(msg.Value)
			)
			if !ok {
				fmt.Fprintf(os.Stderr, "unexpected closed messages chan")
				return
			}

			m := consumedMessage{msg.Partition, msg.Offset, &k, &v}
			if buf, err = json.Marshal(m); err != nil {
				fmt.Fprintf(os.Stderr, "Quitting due to unexpected error during marshal: %v\n", err)
				close(c.closer)
				return
			}

			ctx := printContext{line: string(buf), done: make(chan struct{})}
			out <- ctx
			<-ctx.done

			if end > 0 && msg.Offset >= end {
				return
			}
		}
	}
}

func (c *consume) findPartitions() []int32 {
	var (
		all []int32
		res []int32
		err error
	)
	if all, err = c.consumer.Partitions(c.topic); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read partitions for topic %v err=%v\n", c.topic, err)
		os.Exit(1)
	}

	if _, hasDefault := c.offsets[-1]; hasDefault {
		return all
	}

	for _, p := range all {
		if _, ok := c.offsets[p]; ok {
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

The default is to consume from the oldest offset on every partition for the given topic.

 - partition is the numeric identifier for a partition. You can use "all" to
   specify a default interval for all partitions.

 - start is the included offset where consumption should start.

 - end is the included offset where consumption should end.

The following syntax is supported for each offset:

  (oldest|newest)?(+|-)?(\d+)?

 - "oldest" and "newest" refer to the oldest and newest offsets known for a
   given partition.

 - You can use "+" with a numeric value to skip the given number of messages
   since the oldest offset. For example, "1=+20" will skip 20 offset value since
   the oldest offset for partition 1.

 - You can use "-" with a numeric value to refer to only the given number of
   messages before the newest offset. For example, "1=-10" will refer to the
   last 10 offset values before the newest offset for partition 1.

 - Relative offsets are based on numeric values and will not take skipped
   offsets (e.g. due to compaction) into account.

 - Given only a numeric value, it is interpreted as an absolute offset value.

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
