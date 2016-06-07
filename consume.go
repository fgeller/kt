package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type offsetType int

const (
	absOffset offsetType = iota
	relOffset
)

type offset struct {
	typ   offsetType
	start int64
	diff  int64
}

func (o offset) value(clientMaker func() sarama.Client, partition int32) (int64, error) {
	if o.typ == absOffset {
		return o.start, nil
	}

	if o.start == sarama.OffsetNewest || o.start == sarama.OffsetOldest {
		v, err := clientMaker().GetOffset(config.consume.topic, partition, o.start)
		if err != nil {
			return 0, err
		}
		if o.start == sarama.OffsetNewest {
			v = v - 1
		}
		return v + o.diff, nil
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
	args    struct {
		topic   string
		brokers string
		timeout time.Duration
		offsets string
	}
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

	value, err := strconv.ParseInt(intStr, 10, 64)
	if err != nil && len(intStr) > 0 {
		return result, fmt.Errorf("Invalid partition [%v]", str)
	}
	result.start = value

	result.diff = int64(0)

	result.typ = absOffset
	if len(qualifierStr) > 0 {
		result.typ = relOffset
		result.diff = value
		result.start = sarama.OffsetOldest
		if qualifierStr == "-" {
			result.start = sarama.OffsetNewest
			result.diff = -result.diff
		}
	}

	switch startStr {
	case "newest":
		result.typ = relOffset
		result.start = sarama.OffsetNewest
	case "oldest":
		result.typ = relOffset
		result.start = sarama.OffsetOldest
	}

	return result, nil
}

func parseOffsets(str string) (map[int32]interval, error) {
	defaultInterval := interval{
		start: offset{typ: relOffset, start: sarama.OffsetOldest},
		end:   offset{typ: absOffset, start: 1<<63 - 1},
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

func consumeParseArgs() {
	var err error
	envTopic := os.Getenv("KT_TOPIC")
	if config.consume.args.topic == "" {
		if envTopic == "" {
			failStartup("Topic name is required.")
		} else {
			config.consume.args.topic = envTopic
		}
	}
	config.consume.topic = config.consume.args.topic

	envBrokers := os.Getenv("KT_BROKERS")
	if config.consume.args.brokers == "" {
		if envBrokers != "" {
			config.consume.args.brokers = envBrokers
		} else {
			config.consume.args.brokers = "localhost:9092"
		}
	}
	config.consume.brokers = strings.Split(config.consume.args.brokers, ",")
	for i, b := range config.consume.brokers {
		if !strings.Contains(b, ":") {
			config.consume.brokers[i] = b + ":9092"
		}
	}

	config.consume.offsets, err = parseOffsets(config.consume.args.offsets)
	if err != nil {
		failStartup(fmt.Sprintf("%s", err))
	}
}

func consumeFlags() *flag.FlagSet {
	flags := flag.NewFlagSet("consume", flag.ExitOnError)
	flags.StringVar(&config.consume.args.topic, "topic", "", "Topic to consume (required).")
	flags.StringVar(&config.consume.args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	flags.StringVar(&config.consume.args.offsets, "offsets", "", "Specifies what messages to read by partition and offset range (defaults to all).")
	flags.DurationVar(&config.consume.timeout, "timeout", time.Duration(0), "Timeout after not reading messages (default 0 to disable).")

	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of consume:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, `
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

`)

		os.Exit(2)
	}

	return flags
}

func consumeCommand() command {

	return command{
		flags:     consumeFlags(),
		parseArgs: consumeParseArgs,
		run: func(closer chan struct{}) {

			consumer, err := sarama.NewConsumer(config.consume.brokers, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to create consumer err=%v\n", err)
				os.Exit(1)
			}
			defer consumer.Close()

			partitions := findPartitions(consumer, config.consume)
			if len(partitions) == 0 {
				fmt.Fprintf(os.Stderr, "Found no partitions to consume.\n")
				os.Exit(1)
			}

			consume(config.consume, closer, consumer, partitions)
		},
	}
}

func clientMaker() sarama.Client {
	client, err := sarama.NewClient(config.consume.brokers, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client err=%v\n", err)
		os.Exit(1)
	}

	return client
}

func consume(
	config consumeConfig,
	closer chan struct{},
	consumer sarama.Consumer,
	partitions []int32,
) {
	var wg sync.WaitGroup
	out := make(chan string)
	go func() {
		for {
			select {
			case m := <-out:
				fmt.Println(m)
			}
		}
	}()

	for _, partition := range partitions {
		wg.Add(1)
		go func(p int32) {
			defer wg.Done()
			consumePartition(config, closer, out, consumer, p)
		}(partition)
	}
	wg.Wait()
}

func consumePartition(
	config consumeConfig,
	closer chan struct{},
	out chan string,
	consumer sarama.Consumer,
	partition int32,
) {

	offsets, ok := config.offsets[partition]
	if !ok {
		offsets, ok = config.offsets[-1]
	}

	start, err := offsets.start.value(clientMaker, partition)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read start offset for partition %v err=%v\n", partition, err)
		return
	}

	end, err := offsets.end.value(clientMaker, partition)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read end offset for partition %v err=%v\n", partition, err)
		return
	}

	partitionConsumer, err := consumer.ConsumePartition(
		config.topic,
		partition,
		start,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to consume partition %v err=%v\n", partition, err)
		return
	}

	consumePartitionLoop(closer, out, partitionConsumer, partition, end)
}

func consumePartitionLoop(
	closer chan struct{},
	out chan string,
	pc sarama.PartitionConsumer,
	p int32,
	end int64,
) {
	for {
		timeout := make(<-chan time.Time)
		if config.consume.timeout > 0 {
			timeout = time.After(config.consume.timeout)
		}

		select {
		case <-timeout:
			log.Printf("Consuming from partition [%v] timed out.", p)
			pc.Close()
			return
		case <-closer:
			pc.Close()
			return
		case msg, ok := <-pc.Messages():
			if ok {
				out <- fmt.Sprintf(
					`{"partition":%v,"offset":%v,"key":%#v,"message":%#v}`,
					msg.Partition,
					msg.Offset,
					string(msg.Key),
					string(msg.Value),
				)
			}
			if end > 0 && msg.Offset >= end {
				pc.Close()
				return
			}
		}
	}
}

func findPartitions(consumer sarama.Consumer, config consumeConfig) []int32 {
	allPartitions, err := consumer.Partitions(config.topic)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read partitions for topic %v err=%v\n", config.topic, err)
		os.Exit(1)
	}

	_, hasDefaultOffset := config.offsets[-1]
	partitions := []int32{}
	if !hasDefaultOffset {
		for _, p := range allPartitions {
			_, ok := config.offsets[p]
			if ok {
				partitions = append(partitions, p)
			}
		}
	} else {
		partitions = allPartitions
	}

	return partitions
}
