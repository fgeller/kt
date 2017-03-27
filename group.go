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

	"github.com/Shopify/sarama"
)

type groupCmd struct {
	brokers   []string
	group     string
	filter    *regexp.Regexp
	topic     string
	partition int32
	reset     int64
	verbose   bool
	version   sarama.KafkaVersion
	offsets   bool

	client        sarama.Client
	offsetManager sarama.OffsetManager

	q chan struct{}
}

type group struct {
	Name    string                `json:"name"`
	Topic   string                `json:"topic,omitempty"`
	Offsets map[int32]groupOffset `json:"offsets,omitempty"`
}

type groupOffset struct {
	Offset int64 `json:"offset"`
	Lag    int64 `json:"lag"`
}

func (cmd *groupCmd) run(args []string, q chan struct{}) {
	var err error

	cmd.parseArgs(args)
	cmd.q = q

	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	if cmd.client, err = sarama.NewClient(cmd.brokers, cmd.saramaConfig()); err != nil {
		failf("failed to create client err=%v", err)
	}

	brokers := cmd.findBrokers()
	if len(brokers) == 0 {
		failf("failed to find brokers")
	}

	groups := []string{cmd.group}
	if cmd.group == "" {
		groups = []string{}
		for _, g := range cmd.findGroups(brokers) {
			if cmd.filter.MatchString(g) {
				groups = append(groups, g)
			}
		}
	}
	fmt.Fprintf(os.Stderr, "found %v groups\n", len(groups))

	topics := []string{cmd.topic}
	if cmd.topic == "" {
		topics = cmd.fetchTopics()
	}
	fmt.Fprintf(os.Stderr, "found %v topics\n", len(topics))

	if !cmd.offsets {
		for i, grp := range groups {
			buf, _ := json.Marshal(group{Name: grp})
			fmt.Println(string(buf))
			if cmd.verbose {
				fmt.Fprintf(os.Stderr, "%v/%v\n", i+1, len(groups))
			}
		}
		return
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(groups) * len(topics))
	for _, grp := range groups {
		for _, top := range topics {
			go func(grp, topic string) {
				target := group{Name: grp, Topic: topic, Offsets: map[int32]groupOffset{}}
				cmd.fetchGroupTopicOffset(target.Offsets, grp, topic)
				if len(target.Offsets) > 0 {
					buf, _ := json.Marshal(target)
					fmt.Println(string(buf))
				}
				wg.Done()
			}(grp, top)
		}
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-cmd.q:
	case <-done:
	}
}

func (cmd *groupCmd) fetchGroupTopicOffset(target map[int32]groupOffset, grp, top string) {
	parts := []int32{cmd.partition}
	if cmd.partition == -23 {
		parts = cmd.fetchPartitions(top)
	}
	wg := &sync.WaitGroup{}
	wg.Add(len(parts))
	for _, part := range parts {
		go cmd.fetchGroupOffset(wg, target, grp, top, part)
	}
	wg.Wait()
}
func (cmd *groupCmd) fetchGroupOffset(wg *sync.WaitGroup, target map[int32]groupOffset, grp, top string, part int32) {
	var (
		err           error
		groupOff      int64
		partOff       int64
		offsetManager sarama.OffsetManager
	)

	defer wg.Done()

	if offsetManager, err = sarama.NewOffsetManagerFromClient(grp, cmd.client); err != nil {
		failf("failed to create client err=%v", err)
	}
	pom, err := offsetManager.ManagePartition(top, part)
	if err != nil {
		failf("failed to manage partition group=%s topic=%s partition=%d err=%v", grp, top, part, err)
	}
	groupOff, _ = pom.NextOffset()
	if groupOff == sarama.OffsetNewest {
		// no offset recorded yet
		return
	}

	if cmd.reset > 0 || cmd.reset == sarama.OffsetNewest || cmd.reset == sarama.OffsetOldest {
		groupOff = cmd.reset
		pom.MarkOffset(cmd.reset, "")
	}

	if partOff, err = cmd.client.GetOffset(top, part, sarama.OffsetNewest); err != nil {
		failf("failed to read partition offset for topic=%s partition=%d err=%v", top, part, err)
	}

	target[part] = groupOffset{Offset: groupOff, Lag: partOff - groupOff}
	logClose("partition offset manager", pom)
	logClose("offset manager", offsetManager)
}

func (cmd *groupCmd) fetchTopics() []string {
	tps, err := cmd.client.Topics()
	if err != nil {
		failf("failed to read topics err=%v", err)
	}
	return tps
}

func (cmd *groupCmd) fetchPartitions(top string) []int32 {
	ps, err := cmd.client.Partitions(top)
	if err != nil {
		failf("failed to read partitions for topic=%s err=%v", top, err)
	}
	return ps
}

type findGroupResult struct {
	done  bool
	group string
}

func (cmd *groupCmd) findGroups(brokers []*sarama.Broker) []string {
	var (
		doneCount int
		groups    = []string{}
		results   = make(chan findGroupResult)
		errs      = make(chan error)
	)

	for _, broker := range brokers {
		go cmd.findGroupsOnBroker(broker, results, errs)
	}

awaitGroups:
	for {
		if doneCount == len(brokers) {
			return groups
		}

		select {
		case err := <-errs:
			failf("failed to find groups err=%v", err)
		case res := <-results:
			if res.done {
				doneCount++
				continue awaitGroups
			}
			groups = append(groups, res.group)
		}
	}
}

func (cmd *groupCmd) findGroupsOnBroker(broker *sarama.Broker, results chan findGroupResult, errs chan error) {
	var (
		err  error
		resp *sarama.ListGroupsResponse
	)
	if err = cmd.connect(broker); err != nil {
		errs <- fmt.Errorf("failed to connect to broker %#v err=%s\n", broker.Addr(), err)
	}

	if resp, err = broker.ListGroups(&sarama.ListGroupsRequest{}); err != nil {
		errs <- fmt.Errorf("failed to list brokers on %#v err=%v", broker.Addr(), err)
	}

	if resp.Err != sarama.ErrNoError {
		errs <- fmt.Errorf("failed to list brokers on %#v err=%v", broker.Addr(), resp.Err)
	}

	for name := range resp.Groups {
		results <- findGroupResult{group: name}
	}
	results <- findGroupResult{done: true}
}

func (cmd *groupCmd) connect(broker *sarama.Broker) error {
	if ok, _ := broker.Connected(); ok {
		return nil
	}

	if err := broker.Open(cmd.saramaConfig()); err != nil {
		return err
	}

	connected, err := broker.Connected()
	if err != nil {
		return err
	}

	if !connected {
		return fmt.Errorf("failed to connect broker %#v", broker.Addr())
	}

	return nil
}

func (cmd *groupCmd) findBrokers() []*sarama.Broker {
	var (
		err  error
		meta *sarama.MetadataResponse
	)

loop:
	for _, addr := range cmd.brokers {
		select {
		case <-cmd.q:
			fmt.Printf("interrupt - quits.")
			os.Exit(1)
		default:
		}
		broker := sarama.NewBroker(addr)
		if err = cmd.connect(broker); err != nil {
			fmt.Fprintf(os.Stderr, "failed to open broker connection to %#v err=%s\n", addr, err)
			continue loop
		}

		if meta, err = broker.GetMetadata(&sarama.MetadataRequest{Topics: []string{}}); err != nil {
			fmt.Fprintf(os.Stderr, "failed to request metadata from %#v. err=%s\n", addr, err)
			continue loop
		}

		return meta.Brokers
	}

	return []*sarama.Broker{}
}

func (cmd *groupCmd) saramaConfig() *sarama.Config {
	var (
		err error
		usr *user.User
		cfg = sarama.NewConfig()
	)

	cfg.Version = cmd.version
	if usr, err = user.Current(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-group-" + sanitizeUsername(usr.Username)

	return cfg
}

func (cmd *groupCmd) failStartup(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	failf("use \"kt group -help\" for more information")
}

func (cmd *groupCmd) parseArgs(as []string) {
	var (
		err  error
		args = cmd.parseFlags(as)
	)

	envTopic := os.Getenv("KT_TOPIC")
	if args.topic == "" {
		args.topic = envTopic
	}

	cmd.topic = args.topic
	cmd.group = args.group
	cmd.verbose = args.verbose
	cmd.offsets = args.offsets
	cmd.version = kafkaVersion(args.version)
	cmd.partition = int32(args.partition)

	if cmd.filter, err = regexp.Compile(args.filter); err != nil {
		failf("filter regexp invalid err=%v", err)
	}

	if args.reset != "" && (args.topic == "" || args.partition == -23 || args.group == "") {
		failf("group, topic, partition are required to reset offsets")
	}

	switch args.reset {
	case "newest":
		cmd.reset = sarama.OffsetNewest
	case "oldest":
		cmd.reset = sarama.OffsetOldest
	case "":
		// optional flag
	default:
		cmd.reset, err = strconv.ParseInt(args.reset, 10, 64)
		if err != nil {
			if cmd.verbose {
				fmt.Fprintf(os.Stderr, "failed to parse set %#v err=%v", args.reset, err)
			}
			cmd.failStartup(fmt.Sprintf(`set value %#v not valid. either newest, oldest or specific offset expected.`, args.reset))
		}
	}

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
}

type groupArgs struct {
	topic     string
	brokers   string
	partition int
	group     string
	filter    string
	reset     string
	verbose   bool
	version   string
	offsets   bool
}

func (cmd *groupCmd) parseFlags(as []string) groupArgs {
	var args groupArgs
	flags := flag.NewFlagSet("group", flag.ExitOnError)
	flags.StringVar(&args.topic, "topic", "", "Topic to consume (required).")
	flags.StringVar(&args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	flags.StringVar(&args.group, "group", "", "Consumer group name.")
	flags.StringVar(&args.filter, "filter", "", "Regex to filter groups.")
	flags.StringVar(&args.reset, "reset", "", "Target offset to reset for consumer group (newest, oldest, or specific offset)")
	flags.BoolVar(&args.verbose, "verbose", false, "More verbose logging to stderr.")
	flags.StringVar(&args.version, "version", "", "Kafka protocol version")
	flags.IntVar(&args.partition, "partition", -23, "Partition to limit offsets to")
	flags.BoolVar(&args.offsets, "offsets", true, "Controls if offsets should be fetched (defauls to true)")

	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of group:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, groupDocString)
		os.Exit(2)
	}

	_ = flags.Parse(as)
	return args
}

var groupDocString = `
The values for -topic and -brokers can also be set via environment variables KT_TOPIC and KT_BROKERS respectively.
The values supplied on the command line win over environment variable values.

The group command can be used to list groups, their offsets and lag and to reset a group's offset.

To simply list all groups:

kt group

This is faster when not fetching offsets:

kt group -offsets=false

To filter by regex:

kt group -filter specials

To filter by topic:

kt group -topic fav-topic

To reset a consumer group's offset:

kt group -reset 23 -topic fav-topic -group specials -partition 2
`
