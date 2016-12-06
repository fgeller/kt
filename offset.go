package main

import (
	"encoding/json"
	"flag"
	"fmt"
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

type offsetCmd struct {
	flags      *flag.FlagSet
	brokers    []string
	group      string
	setOffsets string
	topic      *regexp.Regexp
	partition  int32
	newOffsets int64
	verbose    bool
	version    sarama.KafkaVersion

	client        sarama.Client
	offsetManager sarama.OffsetManager
}

type offsetArgs struct {
	brokers    string
	group      string
	topic      string
	partition  int
	setOffsets string
	verbose    bool
	version    string
}

type offsets struct {
	ConsumerGroup   string `json:"consumer-group,omitempty"`
	Topic           string `json:"topic"`
	Partition       int32  `json:"partition"`
	PartitionOffset int64  `json:"partition-offset"`
	ConsumerOffset  *int64 `json:"consumer-offset,omitempty"`
}

func (cmd *offsetCmd) parseFlags(as []string) offsetArgs {
	args := offsetArgs{}
	flags := flag.NewFlagSet("offset", flag.ExitOnError)
	flags.StringVar(&args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted.")
	flags.StringVar(&args.group, "group", "", "The name of the consumer group.")
	flags.StringVar(&args.topic, "topic", "", "The full or partial name of topic(s)")
	flags.IntVar(&args.partition, "partition", -1, "The identifier of the partition")
	flags.StringVar(&args.setOffsets, "setConsumerOffsets", "", `Set offsets for the consumer groups to "oldest", "newest", or a specific numerical value. For more accurate modification also specify topic and/or partition`)
	flags.BoolVar(&args.verbose, "verbose", false, "More verbose logging to stderr.")
	flags.StringVar(&args.version, "version", "", "Kafka protocol version")
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of offset:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, offsetDocString)
		os.Exit(2)
	}

	flags.Parse(as)
	return args
}

func (cmd *offsetCmd) parseArgs(as []string) {
	var (
		err  error
		args = cmd.parseFlags(as)
	)

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

	if cmd.topic, err = regexp.Compile(args.topic); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid regex for filter. err=%s\n", err)
		os.Exit(2)
	}

	cmd.partition = int32(args.partition)
	cmd.group = args.group
	cmd.verbose = args.verbose
	cmd.version = kafkaVersion(args.version)
	cmd.setOffsets = args.setOffsets

	switch args.setOffsets {
	case "":
	case "oldest":
		cmd.newOffsets = sarama.OffsetOldest
	case "newest":
		cmd.newOffsets = sarama.OffsetNewest
	default:
		var off int
		if off, err = strconv.Atoi(args.setOffsets); err != nil {
			fmt.Fprintf(os.Stderr, `Invalid value for setting the offset. possible values are "oldest", "newest", or any numerical value. err=%s\n`, err)
			os.Exit(2)
		}
		cmd.newOffsets = int64(off)
	}
}

func (cmd *offsetCmd) mkClient() {
	var (
		err error
		usr *user.User
		cfg = sarama.NewConfig()
	)

	cfg.Version = cmd.version
	if usr, err = user.Current(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-offset-" + usr.Username

	if cmd.verbose {
		fmt.Fprintf(os.Stderr, "sarama client configuration %#v\n", cfg)
	}

	if cmd.client, err = sarama.NewClient(cmd.brokers, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client err=%v\n", err)
		os.Exit(1)
	}

	if cmd.offsetManager, err = sarama.NewOffsetManagerFromClient(cmd.group, cmd.client); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create offset manager for group=%s err=%v\n", cmd.group, err)
		os.Exit(1)
	}
}

func (cmd *offsetCmd) run(as []string, closer chan struct{}) {
	var (
		tps []string
		err error
	)

	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	cmd.mkClient()
	defer cmd.client.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	out := make(chan printContext)
	done := make(chan bool)

	go func(out chan printContext, done chan bool) {
		if tps, err = cmd.client.Topics(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read topics err=%v\n", err)
			os.Exit(1)
		}

		for _, t := range tps {
			if !cmd.topic.MatchString(t) {
				continue
			}

			cmd.offsetsForTopic(t, out)
		}

		done <- true
	}(out, done)

printLoop:
	for {
		select {
		case m := <-out:
			fmt.Println(m)
		case <-done:
			break printLoop
		case <-closer:
			break printLoop
		}
	}
}

func (cmd *offsetCmd) offsetsForTopic(topic string, out chan printContext) {
	var (
		ps  []int32
		err error
	)

	if ps, err = cmd.client.Partitions(topic); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read partitions for topic=%s err=%v\n", topic, err)
		os.Exit(1)
	}

	for _, p := range ps {
		if cmd.partition == -1 || cmd.partition == p {
			cmd.offsetsForPartition(topic, p, out)
		}
	}
}

// offsetsForPartition processes the offsets for a given partition of a given topic
// if a group ID is passed, it will process the offsets for a consumer group
func (cmd *offsetCmd) offsetsForPartition(topic string, partition int32, out chan printContext) {
	po, err := cmd.client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read offsets for topic=%s partition=%d err=%v\n", topic, partition, err)
		os.Exit(1)
	}

	if cmd.group != "" {
		cmd.offsetsForConsumer(topic, partition, po, out)
		return
	}

	printOffset(offsets{Topic: topic, Partition: partition, PartitionOffset: po}, out)
}

// offsetsForConsumer processes the consumer group offsets for a given partition of a given topic
func (cmd *offsetCmd) offsetsForConsumer(topic string, partition int32, partitionOffset int64, out chan printContext) {

	if cmd.setOffsets != "" {
		cmd.setConsumerOffsets(topic, partition)
		return
	}

	pom, err := cmd.offsetManager.ManagePartition(topic, partition)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read consumer offsets for group=%s topic=%s partition=%d err=%v\n", cmd.group, topic, partition, err)
		os.Exit(1)
	}
	co, _ := pom.NextOffset()
	pom.Close()

	printOffset(offsets{ConsumerGroup: cmd.group, Topic: topic, Partition: partition, PartitionOffset: partitionOffset, ConsumerOffset: &co}, out)
}

// cmd.client.Partitions,
// cmd.client.GetOffset,
// osm.ManagePartition,

// setConsumerOffsets processes the setConsumerOffset flag
func (cmd *offsetCmd) setConsumerOffsets(topic string, partition int32) {
	var (
		err error
		brk *sarama.Broker
		po  = cmd.newOffsets
	)
	if brk, err = cmd.client.Coordinator(cmd.group); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create broker err=%v\n", err)
		os.Exit(1)
	}
	defer logClose("broker", brk)

	memberID, generationID := joinGroup(brk, cmd.group, topic)

	if cmd.newOffsets == sarama.OffsetNewest || cmd.newOffsets == sarama.OffsetOldest {
		if po, err = cmd.client.GetOffset(topic, partition, cmd.newOffsets); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read offsets for topic=%s partition=%d err=%v\n", topic, partition, err)
			os.Exit(1)
		}
	}

	cmd.commitOffset(brk, topic, partition, cmd.group, po, generationID, memberID)
}

// joinGroup joins a consumer group and returns the group memberID and generationID
func joinGroup(broker *sarama.Broker, group string, topic string) (string, int32) {
	var (
		err error
		res *sarama.JoinGroupResponse
	)

	joinGroupReq := &sarama.JoinGroupRequest{
		GroupId:        group,
		SessionTimeout: int32((30 * time.Second) / time.Millisecond),
		ProtocolType:   "consumer",
	}

	meta := &sarama.ConsumerGroupMemberMetadata{
		Version: 1,
		Topics:  []string{topic},
	}

	if err := joinGroupReq.AddGroupProtocolMetadata("range", meta); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to add meta data err=%v\n", err)
		os.Exit(1)
	}

	if err = joinGroupReq.AddGroupProtocolMetadata("roundrobin", meta); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to add meta data err=%v\n", err)
		os.Exit(1)
	}

	if res, err := broker.JoinGroup(joinGroupReq); err != nil || res.Err != sarama.ErrNoError {
		fmt.Fprintf(os.Stderr, "Failed to join consumer group err=%v responseErr=%v\n", err, res.Err)
		os.Exit(1)
	}

	return res.MemberId, res.GenerationId
}

// commitOffset sends an offset message to kafka for the given consumer group
func (cmd *offsetCmd) commitOffset(broker *sarama.Broker, topic string, partition int32, group string, offset int64, generationID int32, memberID string) {

	var (
		ocr *sarama.OffsetCommitResponse
		err error
	)

	v := int16(0)
	if cmd.version.IsAtLeast(sarama.V0_8_2_0) {
		v = 1
	}
	if cmd.version.IsAtLeast(sarama.V0_9_0_0) {
		v = 2
	}

	req := &sarama.OffsetCommitRequest{
		Version:                 v,
		ConsumerGroup:           group,
		ConsumerGroupGeneration: generationID,
		ConsumerID:              memberID,
		RetentionTime:           -1,
	}
	req.AddBlock(topic, partition, offset, 0, "")

	if ocr, err = broker.CommitOffset(req); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to commit offsets. err=%v\n", err)
		os.Exit(1)
	}

	for topic, perrs := range ocr.Errors {
		for partition, kerr := range perrs {
			if kerr != sarama.ErrNoError {
				fmt.Fprintf(os.Stderr, "Failed to commit offsets topic=%s, partition=%s. err=%v\n", topic, partition, err)
				os.Exit(1)
			}
		}
	}
}

func printOffset(o offsets, out chan printContext) {
	var (
		err error
		buf []byte
	)

	if buf, err = json.Marshal(o); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal JSON for consumer group %+v. err=%v\n", o, err)
		return
	}

	ctx := printContext{string(buf), make(chan struct{})}
	out <- ctx
	<-ctx.done
}

var offsetDocString = `
The values for -brokers can also be set via the environment variable KT_BROKERS respectively.
The values supplied on the command line win over environment variable values.

Offsets are listed for partitions/consumer groups. This can be filtered by the flags [brokers], [topic], and [partition].

This tool also offers the option to update these by adding the [setConsumerOffsets] flag.

The following syntax is supported for setConsumerOffsets:

  (oldest|newest|\d+)?

 - "oldest" and "newest" refer to the oldest and newest offsets known for a
   given partition.

 - Given only a numeric value, it is interpreted as an absolute offset value.
`
