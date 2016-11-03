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

type offsetConfig struct {
	flags     *flag.FlagSet
	brokers   []string
	group     string
	topic     *regexp.Regexp
	partition int32
	setOldest bool
	setNewest bool
	set       int64
	verbose   bool
	version   sarama.KafkaVersion
	args      struct {
		brokers   string
		group     string
		topic     string
		partition int
		set       string
		verbose   bool
		version   string
	}
}

type offsets struct {
	ConsumerGroup   string `json:"consumer-group,omitempty"`
	Topic           string `json:"topic"`
	Partition       int32  `json:"partition"`
	PartitionOffset int64  `json:"partition-offset"`
	ConsumerOffset  *int64 `json:"consumer-offset,omitempty"`
}

func offsetCommand() command {
	return command{
		flags:     offsetFlags(),
		parseArgs: offsetParseArgs,
		run:       offsetRun,
	}
}

func offsetFlags() *flag.FlagSet {
	offset := flag.NewFlagSet("offset", flag.ExitOnError)
	offset.StringVar(&config.offset.args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted.")
	offset.StringVar(&config.offset.args.group, "group", "", "The name of the consumer group.")
	offset.StringVar(&config.offset.args.topic, "topic", "", "The full or partial name of topic(s)")
	offset.IntVar(&config.offset.args.partition, "partition", -1, "The identifier of the partition")
	offset.StringVar(&config.offset.args.set, "setConsumerOffsets", "", "Set offsets for the consumer groups to \"oldest\", \"newest\", or a specific numerical value. "+
		"For more accurate modification also specify topic and/or partition")
	offset.BoolVar(&config.offset.args.verbose, "verbose", false, "More verbose logging to stderr.")
	offset.StringVar(&config.offset.args.version, "version", "", "Kafka protocol version")

	offset.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of offset:")
		offset.PrintDefaults()
		fmt.Fprintln(os.Stderr, `
The values for -brokers can also be set via the environment variable KT_BROKERS respectively.
The values supplied on the command line win over environment variable values.

Offsets are listed for partitions/consumer groups. This can be filtered by the flags [brokers], [topic], and [partition].


This tool also offers the option to update these by adding the [setConsumerOffsets] flag.

The following syntax is supported for setConsumerOffsets:

  (oldest|newest)?(\d+)?

 - "oldest" and "newest" refer to the oldest and newest offsets known for a
   given partition.

 - Given only a numeric value, it is interpreted as an absolute offset value.
`)
		os.Exit(2)
	}

	return offset
}

func offsetParseArgs() {
	envBrokers := os.Getenv("KT_BROKERS")
	if config.offset.args.brokers == "" {
		if envBrokers != "" {
			config.offset.args.brokers = envBrokers
		} else {
			config.offset.args.brokers = "localhost:9092"
		}
	}
	config.offset.brokers = strings.Split(config.offset.args.brokers, ",")
	for i, b := range config.offset.brokers {
		if !strings.Contains(b, ":") {
			config.offset.brokers[i] = b + ":9092"
		}
	}

	re, err := regexp.Compile(config.offset.args.topic)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid regex for filter. err=%s\n", err)
		os.Exit(2)
	}
	config.offset.topic = re

	config.offset.partition = int32(config.offset.args.partition)

	config.offset.group = config.offset.args.group

	if config.offset.args.set == "" {
		config.offset.set = -1
	} else if config.offset.args.set == "newest" {
		config.offset.setNewest = true
	} else if config.offset.args.set == "oldest" {
		config.offset.setOldest = true
	} else if newOffset, err := strconv.Atoi(config.offset.args.set); err == nil {
		config.offset.set = int64(newOffset)
	} else {
		fmt.Fprintf(os.Stderr, "Invalid value for setting the offset. possible values are \"oldest\", \"newest\", or any numerical value. err=%s\n", err)
		os.Exit(2)
	}

	config.offset.verbose = config.offset.args.verbose
	config.offset.version = kafkaVersion(config.offset.args.version)
}

func offsetRun(closer chan struct{}) {
	var err error
	if config.offset.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	conf := sarama.NewConfig()
	conf.Version = config.offset.version
	u, err := user.Current()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read current user err=%v", err)
	}
	conf.ClientID = "kt-offset-" + u.Username
	if config.offset.verbose {
		fmt.Fprintf(os.Stderr, "sarama client configuration %#v\n", conf)
	}

	client, err := sarama.NewClient(config.offset.brokers, conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client err=%v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	om, err := sarama.NewOffsetManagerFromClient(config.offset.group, client)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create offset manager for [%s] err=%v\n", config.offset.group, err)
		os.Exit(1)
	}

	getBroker := client.Coordinator
	getPartitions := client.Partitions
	getOffset := client.GetOffset
	getPartitionOffsetManager := om.ManagePartition

	var wg sync.WaitGroup
	wg.Add(1)

	out := make(chan string)
	go func(out chan string) {
		topics, err := client.Topics()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read topics err=%v\n", err)
			os.Exit(1)
		}

		for _, t := range topics {
			if config.offset.args.topic == "" || len(config.offset.topic.FindString(t)) > 0 {
				offsetsForTopic(getBroker, getPartitions, getOffset, getPartitionOffsetManager, config.offset.group, t, out)
			}
		}

		wg.Done()
	}(out)

	// print to console
	go func() {
		for {
			select {
			case m := <-out:
				fmt.Println(m)
			}
		}
	}()
	wg.Wait()
}

// offsetsForTopic processes the offsets for a given topic
func offsetsForTopic(
	getBroker func(string) (*sarama.Broker, error),
	getPartitions func(string) ([]int32, error),
	getOffset func(string, int32, int64) (int64, error),
	getPartitionOffsetManager func(string, int32) (sarama.PartitionOffsetManager, error),
	group string,
	topic string,
	out chan string,
) {
	partitions, err := getPartitions(topic)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read partitions for [%s] err=%v\n", topic, err)
		os.Exit(1)
	}

	for _, p := range partitions {
		if config.offset.partition == -1 || config.offset.partition == p {
			offsetsForPartition(getBroker, getOffset, getPartitionOffsetManager, group, topic, p, out)
		}
	}
}

// offsetsForPartition processes the offsets for a given partition of a given topic
// if a group ID is passed, it will process the offsets for a consumer group
func offsetsForPartition(
	getBroker func(string) (*sarama.Broker, error),
	getOffset func(string, int32, int64) (int64, error),
	getPartitionOffsetManager func(string, int32) (sarama.PartitionOffsetManager, error),
	group string,
	topic string,
	partition int32,
	out chan string,
) {
	po, err := getOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read offsets for [%s][%d] err=%v\n", topic, partition, err)
		os.Exit(1)
	}

	if group == "" {
		printOffset(offsets{
			Topic:           topic,
			Partition:       partition,
			PartitionOffset: po,
		}, out)
	} else {
		offsetsForConsumer(getBroker, getOffset, getPartitionOffsetManager, group, topic, partition, po, out)
	}
}

// offsetsForConsumer processes the consumer group offsets for a given partition of a given topic
func offsetsForConsumer(
	getBroker func(string) (*sarama.Broker, error),
	getOffset func(string, int32, int64) (int64, error),
	getPartitionOffsetManager func(string, int32) (sarama.PartitionOffsetManager, error),
	group string,
	topic string,
	partition int32,
	partitionOffset int64,
	out chan string,
) {

	if config.offset.args.set != "" {
		setConsumerOffsets(getBroker, getOffset, topic, partition, group)
	}

	pom, err := getPartitionOffsetManager(topic, partition)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read consumer offsets for [%s] for [%s][%d] err=%v\n", config.offset.group, topic, partition, err)
		os.Exit(1)
	}
	co, _ := pom.NextOffset()
	pom.Close()

	printOffset(offsets{
		ConsumerGroup:   group,
		Topic:           topic,
		Partition:       partition,
		PartitionOffset: partitionOffset,
		ConsumerOffset:  &co,
	}, out)
}

// setConsumerOffsets processes the setConsumerOffset flag
func setConsumerOffsets(
	getBroker func(string) (*sarama.Broker, error),
	getOffset func(string, int32, int64) (int64, error),
	topic string,
	partition int32,
	group string,
) {

	broker, err := getBroker(group)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create broker err=%v\n", err)
		os.Exit(1)
	}

	memberID, generationID := joinGroup(broker, group, topic)

	var po int64
	if config.offset.setOldest {
		po, err = getOffset(topic, partition, sarama.OffsetOldest)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read offsets for [%s][%d] err=%v\n", topic, partition, err)
			os.Exit(1)
		}
	} else if config.offset.setNewest {
		po, err = getOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read offsets for [%s][%d] err=%v\n", topic, partition, err)
			os.Exit(1)
		}
	} else {
		po = config.offset.set
	}

	commitOffset(broker, topic, partition, group, po, generationID, memberID)

	broker.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to close broker err=%v\n", err)
		os.Exit(1)
	}
}

// joinGroup joins a consumer group and returns the group memberID and generationID
func joinGroup(broker *sarama.Broker, group string, topic string) (memberID string, generationID int32) {
	joinGroupReq := &sarama.JoinGroupRequest{
		GroupId:        group,
		SessionTimeout: int32((30 * time.Second) / time.Millisecond),
		ProtocolType:   "consumer",
	}

	meta := &sarama.ConsumerGroupMemberMetadata{
		Version: 1,
		Topics:  []string{topic},
	}
	err := joinGroupReq.AddGroupProtocolMetadata("range", meta)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to add meta data err=%v\n", err)
		os.Exit(1)
	}
	err = joinGroupReq.AddGroupProtocolMetadata("roundrobin", meta)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to add meta data err=%v\n", err)
		os.Exit(1)
	}

	resp, err := broker.JoinGroup(joinGroupReq)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create join consumer group err=%v\n", err)
		os.Exit(1)
	} else if resp.Err != sarama.ErrNoError {
		fmt.Fprintf(os.Stderr, "Failed to create join consumer group err=%v\n", resp.Err)
		os.Exit(1)
	}

	memberID = resp.MemberId
	generationID = resp.GenerationId
	return memberID, generationID
}

// commitOffset sends an offset message to kafka for the given consumer group
func commitOffset(broker *sarama.Broker, topic string, partition int32, group string, offset int64, generationID int32, memberID string) {

	v := int16(2)
	if config.offset.version == sarama.V0_8_2_0 || config.offset.version == sarama.V0_8_2_1 {
		v = 1
	}

	req := &sarama.OffsetCommitRequest{
		Version:                 v,
		ConsumerGroup:           group,
		ConsumerGroupGeneration: generationID,
		ConsumerID:              memberID,
		RetentionTime:           -1,
	}
	req.AddBlock(topic, partition, offset, 0, "")

	offsetResp, err := broker.CommitOffset(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to commit offsets. err=%v\n", err)
		os.Exit(1)
	} else if len(offsetResp.Errors) > 0 {
		for topic, perrs := range offsetResp.Errors {
			for partition, kerr := range perrs {
				if kerr != sarama.ErrNoError {
					fmt.Fprintf(os.Stderr, "Failed to commit offsets [%s][%s]. err=%v\n", topic, partition, err)
					os.Exit(1)
				}
			}
		}
	}
}

// printOffset sends the JSON offsets to the out channel
func printOffset(o offsets, out chan string) {
	var byts []byte
	var err error

	if byts, err = json.Marshal(o); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal JSON for consumer group %v+. err=%v\n", o, err)
		return
	}

	out <- string(byts)
}
