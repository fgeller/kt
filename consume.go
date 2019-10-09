package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type consumeCmd struct {
	commonFlags
	topic           string
	offsets         string
	timeout         time.Duration
	encodeValueType string
	encodeKeyType   string
	pretty          bool
	follow          bool

	encodeValue func([]byte) *string
	encodeKey   func([]byte) *string
	client      sarama.Client
	consumer    sarama.Consumer
}

func (cmd *consumeCmd) addFlags(flags *flag.FlagSet) {
	cmd.commonFlags.addFlags(flags)
	flags.StringVar(&cmd.topic, "topic", "", "Topic to consume (required).")
	flags.StringVar(&cmd.offsets, "offsets", "", "Specifies what messages to read by partition and offset range (defaults to all).")
	flags.DurationVar(&cmd.timeout, "timeout", time.Duration(0), "Timeout after not reading messages (default 0 to disable).")
	flags.BoolVar(&cmd.pretty, "pretty", true, "Control output pretty printing.")
	flags.BoolVar(&cmd.follow, "f", false, "Follow topic by waiting new messages (default is to stop at end of topic)")
	flags.StringVar(&cmd.encodeValueType, "encodevalue", "string", "Present message value as (string|hex|base64), defaults to string.")
	flags.StringVar(&cmd.encodeKeyType, "encodekey", "string", "Present message key as (string|hex|base64), defaults to string.")

	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of consume:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, consumeDocString)
	}
}

func (cmd *consumeCmd) environFlags() map[string]string {
	return map[string]string{
		"topic":   "KT_TOPIC",
		"brokers": "KT_BROKERS",
	}
}

func (cmd *consumeCmd) run(args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("unexpected argument to consume command")
	}
	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	var err error
	cmd.encodeValue, err = encoderForType(cmd.encodeValueType)
	if err != nil {
		return fmt.Errorf("bad -encodevalue argument: %v", err)
	}
	cmd.encodeKey, err = encoderForType(cmd.encodeKeyType)
	if err != nil {
		return fmt.Errorf("bad -encodekey argument: %v", err)
	}
	offsets, err := parseOffsets(cmd.offsets, time.Now())
	if err != nil {
		return err
	}
	c, err := cmd.newClient()
	if err != nil {
		return err
	}
	cmd.client = c
	consumer, err := sarama.NewConsumerFromClient(cmd.client)
	if err != nil {
		return fmt.Errorf("cannot create kafka consumer: %v", err)
	}
	cmd.consumer = consumer
	defer logClose("consumer", cmd.consumer)
	resolvedOffsets, limits, err := cmd.resolveOffsets(context.TODO(), offsets)
	if err != nil {
		return fmt.Errorf("cannot resolve offsets: %v", err)
	}
	if err := cmd.consume(resolvedOffsets, limits); err != nil {
		return err
	}
	return nil
}

func (cmd *consumeCmd) newClient() (sarama.Client, error) {
	cfg, err := cmd.saramaConfig("consume")
	if err != nil {
		return nil, err
	}
	client, err := sarama.NewClient(cmd.brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("cannot create kafka client: %v", err)
	}
	return client, nil
}

func (cmd *consumeCmd) consume(partitions map[int32]resolvedInterval, limits map[int32]int64) error {
	// Make a slice of consume partitions so we can easily divide it up for merging.
	// We merge messages up to the partition limits; beyond the limits
	// we produce messages in order.
	consumerChans := make([]<-chan *sarama.ConsumerMessage, 0, len(partitions))
	out := newPrinter(cmd.pretty)
	var wg sync.WaitGroup
	wg.Add(len(partitions))
	for p, interval := range partitions {
		p, interval := p, interval
		if interval.end > limits[p] {
			interval.end = limits[p]
		}
		outc := make(chan *sarama.ConsumerMessage)
		go func() {
			defer wg.Done()
			defer close(outc)
			if err := cmd.consumePartition(outc, p, interval); err != nil {
				warningf("cannot consume partition %v: %v", p, err)
			}
		}()
		consumerChans = append(consumerChans, outc)
	}
	allMsgs := mergeConsumers(consumerChans...)
	for m := range allMsgs {
		out.print(cmd.newConsumedMessage(m))
	}
	wg.Wait()
	// We've got to the end of all partitions; now print messages
	// as soon as they arrive.
	outc := make(chan *sarama.ConsumerMessage)
	for p, interval := range partitions {
		p, interval := p, interval
		if interval.end <= limits[p] {
			// We've already consumed all the required messages, so
			// no need to consume this partition any more.
			continue
		}
		wg.Add(1)
		interval.start = limits[p]
		go func() {
			defer wg.Done()
			if err := cmd.consumePartition(outc, p, interval); err != nil {
				warningf("cannot consume partition %v: %v", p, err)
			}
		}()
	}
	go func() {
		wg.Wait()
		close(outc)
	}()
	for m := range outc {
		out.print(cmd.newConsumedMessage(m))
	}
	return nil
}

func (cmd *consumeCmd) consumePartition(out chan<- *sarama.ConsumerMessage, partition int32, interval resolvedInterval) error {
	if interval.start >= interval.end {
		return nil
	}
	pc, err := cmd.consumer.ConsumePartition(cmd.topic, partition, interval.start)
	if err != nil {
		return fmt.Errorf("failed to consume partition %v: %v", partition, err)
	}
	defer logClose(fmt.Sprintf("partition consumer %v", partition), pc)
	var timer *time.Timer
	var timeout <-chan time.Time
	if cmd.timeout > 0 {
		timer = time.NewTimer(cmd.timeout)
		timeout = timer.C
	}
	for {
		if timer != nil {
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(cmd.timeout)
		}
		select {
		case <-timeout:
			return fmt.Errorf("consuming from partition %v timed out after %s", partition, cmd.timeout)
		case err := <-pc.Errors():
			return fmt.Errorf("partition %v consumer encountered error %s", partition, err)
		case msg, ok := <-pc.Messages():
			if !ok {
				return fmt.Errorf("unexpected closed messages chan")
			}
			out <- msg
			if interval.end > 0 && msg.Offset >= interval.end-1 {
				return nil
			}
		}
	}
}

type consumedMessage struct {
	Partition int32      `json:"partition"`
	Offset    int64      `json:"offset"`
	Key       *string    `json:"key"`
	Value     *string    `json:"value"`
	Time      *time.Time `json:"time,omitempty"`
}

func (cmd *consumeCmd) newConsumedMessage(m *sarama.ConsumerMessage) consumedMessage {
	result := consumedMessage{
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       cmd.encodeKey(m.Key),
		Value:     cmd.encodeValue(m.Value),
	}
	if !m.Timestamp.IsZero() {
		t := m.Timestamp.UTC()
		result.Time = &t
	}
	return result
}

// mergeConsumers merges all the given channels in timestamp order
// until all existing messages have been received; it then produces
// messages as soon as they're received.
func mergeConsumers(chans ...<-chan *sarama.ConsumerMessage) <-chan *sarama.ConsumerMessage {
	switch len(chans) {
	case 0:
		// Shouldn't happen but be defensive.
		c := make(chan *sarama.ConsumerMessage)
		close(c)
		return c
	case 1:
		return chans[0]
	case 2:
		c0, c1 := chans[0], chans[1]
		out := make(chan *sarama.ConsumerMessage, 1)
		go mergeMessages([2]<-chan *sarama.ConsumerMessage{c0, c1}, out)
		return out
	default:
		n := len(chans) / 2
		return mergeConsumers(
			mergeConsumers(chans[0:n]...),
			mergeConsumers(chans[n:]...),
		)
	}
}

// merge merges two message channels in timestamp order,
// writing the result to out, which is closed when both
// input channels are closed.
func mergeMessages(cs [2]<-chan *sarama.ConsumerMessage, out chan<- *sarama.ConsumerMessage) {
	defer close(out)

	var msgs [2]*sarama.ConsumerMessage
	// get returns a message from cs[i], reading
	// from the channel if a message isn't already available.
	// If the channel is closed, it sets it to nil.
	get := func(i int) *sarama.ConsumerMessage {
		if msgs[i] != nil {
			return msgs[i]
		}
		m, ok := <-cs[i]
		msgs[i] = m
		if !ok {
			cs[i] = nil
		}
		return m
	}
	for cs[0] != nil && cs[1] != nil {
		if m0, m1 := get(0), get(1); m0 != nil && m1 != nil {
			if m0.Timestamp.Before(m1.Timestamp) {
				out <- m0
				msgs[0] = nil
			} else {
				out <- m1
				msgs[1] = nil
			}
		}
	}
	// One or both of the channels has been closed.
	var c <-chan *sarama.ConsumerMessage
	for i := range cs {
		if msgs[i] != nil {
			// There's a message remaining in the other channel; send it.
			out <- msgs[i]
		}
		if cs[i] != nil {
			c = cs[i]
		}
	}
	if c != nil {
		// Read the rest of the messages from the remaining unclosed channel.
		for m := range c {
			out <- m
		}
	}
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
