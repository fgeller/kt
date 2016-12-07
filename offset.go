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

	out chan printContext

	client        sarama.Client
	broker        *sarama.Broker
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
		failf("invalid regex for filter err=%s", err)
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
			failf(`invalid value for setting the offset, possible values are "oldest", "newest", or any numerical value err=%s`, err)
		}
		cmd.newOffsets = int64(off)
	}
}

func (cmd *offsetCmd) connect() {
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
		failf("failed to create client err=%v", err)
	}

	if cmd.offsetManager, err = sarama.NewOffsetManagerFromClient(cmd.group, cmd.client); err != nil {
		failf("failed to create offset manager for group=%s err=%v", cmd.group, err)
	}

	if cmd.group != "" {
		if cmd.broker, err = cmd.client.Coordinator(cmd.group); err != nil {
			failf("failed to create broker err=%v", err)
		}
		defer logClose("broker", cmd.broker)
	}
}

func (cmd *offsetCmd) run(as []string, q chan struct{}) {
	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	cmd.connect()
	defer logClose("client", cmd.client)

	cmd.out = make(chan printContext)
	go print(cmd.out)

	for _, top := range cmd.fetchTopics() {
		if cmd.topic.MatchString(top) {
			for _, prt := range cmd.fetchPartitions(top) {
				if cmd.partition == -1 || cmd.partition == prt {
					if cmd.setOffsets != "" {
						cmd.setConsumerOffsets(top, prt)
						continue
					}

					po, co := cmd.fetchOffsets(top, prt)
					off := offsets{Topic: top, Partition: prt, PartitionOffset: po}
					if cmd.group != "" {
						off.ConsumerGroup = cmd.group
						off.ConsumerOffset = &co
					}
					cmd.printOffset(off)
					select {
					case <-q:
						fmt.Fprintf(os.Stderr, "received signal, quitting.")
						return
					default:
					}
				}
			}
		}
	}
}

func (cmd *offsetCmd) fetchTopics() []string {
	tps, err := cmd.client.Topics()
	if err != nil {
		failf("failed to read topics err=%v", err)
	}
	return tps
}

func (cmd *offsetCmd) fetchPartitions(top string) []int32 {
	ps, err := cmd.client.Partitions(top)
	if err != nil {
		failf("failed to read partitions for topic=%s err=%v", top, err)
	}
	return ps
}

func (cmd *offsetCmd) fetchOffsets(top string, prt int32) (partitionOffset int64, groupOffset int64) {
	var err error
	if partitionOffset, err = cmd.client.GetOffset(top, prt, sarama.OffsetNewest); err != nil {
		failf("failed to read offsets for topic=%s partition=%d err=%v", top, prt, err)
	}

	if cmd.group == "" {
		return partitionOffset, 0
	}

	return partitionOffset, cmd.fetchGroupOffset(top, prt, partitionOffset)
}

func (cmd *offsetCmd) fetchGroupOffset(top string, prt int32, po int64) int64 {
	pom, err := cmd.offsetManager.ManagePartition(top, prt)
	if err != nil {
		failf("failed to read consumer offsets for group=%s topic=%s partition=%d err=%v", cmd.group, top, prt, err)
	}
	co, _ := pom.NextOffset()
	pom.Close()

	return co
}

func (cmd *offsetCmd) setConsumerOffsets(top string, prt int32) {
	var (
		err error
		po  = cmd.newOffsets
	)

	memberID, generationID := cmd.join(top)
	if cmd.newOffsets == sarama.OffsetNewest || cmd.newOffsets == sarama.OffsetOldest {
		if po, err = cmd.client.GetOffset(top, prt, cmd.newOffsets); err != nil {
			failf("failed to read offsets for topic=%s partition=%d err=%v", top, prt, err)
		}
	}

	cmd.commit(top, prt, po, generationID, memberID)
}

func (cmd *offsetCmd) join(top string) (string, int32) {
	var (
		err error
		res *sarama.JoinGroupResponse
	)

	joinGroupReq := &sarama.JoinGroupRequest{
		GroupId:        cmd.group,
		SessionTimeout: int32((30 * time.Second) / time.Millisecond),
		ProtocolType:   "consumer",
	}

	meta := &sarama.ConsumerGroupMemberMetadata{
		Version: 1,
		Topics:  []string{top},
	}

	if err := joinGroupReq.AddGroupProtocolMetadata("range", meta); err != nil {
		failf("failed to add meta data err=%v", err)
	}

	if err = joinGroupReq.AddGroupProtocolMetadata("roundrobin", meta); err != nil {
		failf("failed to add meta data err=%v", err)
	}

	if res, err := cmd.broker.JoinGroup(joinGroupReq); err != nil || res.Err != sarama.ErrNoError {
		failf("failed to join consumer group err=%v responseErr=%v", err, res.Err)
	}

	return res.MemberId, res.GenerationId
}

func (cmd *offsetCmd) commit(top string, prt int32, offset int64, generationID int32, memberID string) {
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
		ConsumerGroup:           cmd.group,
		ConsumerGroupGeneration: generationID,
		ConsumerID:              memberID,
		RetentionTime:           -1,
	}
	req.AddBlock(top, prt, offset, 0, "")

	if ocr, err = cmd.broker.CommitOffset(req); err != nil {
		failf("failed to commit offsets err=%v", err)
	}

	for topic, perrs := range ocr.Errors {
		for partition, kerr := range perrs {
			if kerr != sarama.ErrNoError {
				failf("failed to commit offsets topic=%s, partition=%s. err=%v", topic, partition, err)
			}
		}
	}
}

func (cmd *offsetCmd) printOffset(o offsets) {
	var (
		err error
		buf []byte
	)

	if buf, err = json.Marshal(o); err != nil {
		failf("failed to marshal JSON for consumer group %#v err=%v", o, err)
	}

	ctx := printContext{string(buf), make(chan struct{})}
	cmd.out <- ctx
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
