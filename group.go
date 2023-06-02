package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
)

type groupCmd struct {
	baseCmd

	brokers      []string
	auth         authConfig
	group        string
	filterGroups *regexp.Regexp
	filterTopics *regexp.Regexp
	topic        string
	partitions   []int32
	reset        int64
	pretty       bool
	version      sarama.KafkaVersion
	offsets      bool

	client sarama.Client
}

type group struct {
	Name    string        `json:"name"`
	Topic   string        `json:"topic,omitempty"`
	Offsets []groupOffset `json:"offsets,omitempty"`
}

type groupOffset struct {
	Partition int32  `json:"partition"`
	Offset    *int64 `json:"offset"`
	Lag       *int64 `json:"lag"`
}

const (
	allPartitionsHuman = "all"
	resetNotSpecified  = -23
)

func (cmd *groupCmd) run(args []string) {
	var err error

	cmd.parseArgs(args)

	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	if cmd.client, err = sarama.NewClient(cmd.brokers, cmd.saramaConfig()); err != nil {
		failf("failed to create client err=%v", err)
	}

	brokers := cmd.client.Brokers()
	cmd.infof("found %v brokers\n", len(brokers))

	groups := []string{cmd.group}
	if cmd.group == "" {
		groups = []string{}
		for _, g := range cmd.findGroups(brokers) {
			if cmd.filterGroups.MatchString(g) {
				groups = append(groups, g)
			}
		}
	}
	cmd.infof("found %v groups\n", len(groups))

	topics := []string{cmd.topic}
	if cmd.topic == "" {
		topics = []string{}
		for _, t := range cmd.fetchTopics() {
			if cmd.filterTopics.MatchString(t) {
				topics = append(topics, t)
			}
		}
	}
	cmd.infof("found %v topics\n", len(topics))

	out := make(chan printContext)
	go print(out, cmd.pretty)

	if !cmd.offsets {
		for i, grp := range groups {
			ctx := printContext{output: group{Name: grp}, done: make(chan struct{})}
			out <- ctx
			<-ctx.done

			cmd.infof("%v/%v\n", i+1, len(groups))
		}
		return
	}

	topicPartitions := map[string][]int32{}
	for _, topic := range topics {
		parts := cmd.partitions
		if len(parts) == 0 {
			parts = cmd.fetchPartitions(topic)
			cmd.infof("found partitions=%v for topic=%v\n", parts, topic)
		}
		topicPartitions[topic] = parts
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(groups) * len(topics))
	for _, grp := range groups {
		for top, parts := range topicPartitions {
			go func(grp, topic string, partitions []int32) {
				cmd.printGroupTopicOffset(out, grp, topic, partitions)
				wg.Done()
			}(grp, top, parts)
		}
	}
	wg.Wait()
}

func (cmd *groupCmd) printGroupTopicOffset(out chan printContext, grp, top string, parts []int32) {
	target := group{Name: grp, Topic: top, Offsets: make([]groupOffset, 0, len(parts))}
	results := make(chan groupOffset)
	done := make(chan struct{})

	wg := &sync.WaitGroup{}
	wg.Add(len(parts))
	for _, part := range parts {
		go cmd.fetchGroupOffset(wg, grp, top, part, results)
	}
	go func() { wg.Wait(); close(done) }()

awaitGroupOffsets:
	for {
		select {
		case res := <-results:
			target.Offsets = append(target.Offsets, res)
		case <-done:
			break awaitGroupOffsets
		}
	}

	if len(target.Offsets) > 0 {
		sort.Slice(target.Offsets, func(i, j int) bool {
			return target.Offsets[j].Partition > target.Offsets[i].Partition
		})
		ctx := printContext{output: target, done: make(chan struct{})}
		out <- ctx
		<-ctx.done
	}
}

func (cmd *groupCmd) resolveOffset(top string, part int32, off int64) int64 {
	resolvedOff, err := cmd.client.GetOffset(top, part, off)
	if err != nil {
		failf("failed to get offset to reset to for partition=%d err=%v", part, err)
	}

	cmd.infof("resolved offset %v for topic=%s partition=%d to %v\n", off, top, part, resolvedOff)

	return resolvedOff
}

func (cmd *groupCmd) fetchGroupOffset(wg *sync.WaitGroup, grp, top string, part int32, results chan<- groupOffset) {
	defer wg.Done()

	cmd.infof("fetching offset information for group=%v topic=%v partition=%v\n", grp, top, part)

	offsetManager, err := sarama.NewOffsetManagerFromClient(grp, cmd.client)
	if err != nil {
		failf("failed to create client err=%v", err)
	}
	defer logClose("offset manager", offsetManager)

	pom, err := offsetManager.ManagePartition(top, part)
	if err != nil {
		failf("failed to manage partition group=%s topic=%s partition=%d err=%v", grp, top, part, err)
	}
	defer logClose("partition offset manager", pom)

	specialOffset := cmd.reset == sarama.OffsetNewest || cmd.reset == sarama.OffsetOldest

	groupOff, _ := pom.NextOffset()
	if cmd.reset >= 0 || specialOffset {
		resolvedOff := cmd.reset
		if specialOffset {
			resolvedOff = cmd.resolveOffset(top, part, cmd.reset)
		}
		if resolvedOff > groupOff {
			pom.MarkOffset(resolvedOff, "")
		} else {
			pom.ResetOffset(resolvedOff, "")
		}

		groupOff = resolvedOff
	}

	// we haven't reset it, and it wasn't set before - lag depends on client's config
	if specialOffset {
		results <- groupOffset{Partition: part}
		return
	}

	partOff := cmd.resolveOffset(top, part, sarama.OffsetNewest)
	lag := partOff - groupOff
	results <- groupOffset{Partition: part, Offset: &groupOff, Lag: &lag}
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

func (cmd *groupCmd) saramaConfig() *sarama.Config {
	var (
		err error
		usr *user.User
		cfg = sarama.NewConfig()
	)

	cfg.Version = cmd.version
	if usr, err = user.Current(); err != nil {
		cmd.infof("Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-group-" + sanitizeUsername(usr.Username)
	cmd.infof("sarama client configuration %#v\n", cfg)

	if err = setupAuth(cmd.auth, cfg); err != nil {
		failf("failed to setup auth err=%v", err)
	}

	return cfg
}

func (cmd *groupCmd) failStartup(msg string) {
	warnf(msg)
	failf("use \"kt group -help\" for more information")
}

func (cmd *groupCmd) parseArgs(as []string) {
	var (
		err  error
		args = cmd.parseFlags(as)
	)

	envTopic := os.Getenv(ENV_TOPIC)
	if args.topic == "" {
		args.topic = envTopic
	}

	cmd.topic = args.topic
	cmd.group = args.group
	cmd.verbose = args.verbose
	cmd.pretty = args.pretty
	cmd.offsets = args.offsets
	cmd.version, err = chooseKafkaVersion(args.version, os.Getenv(ENV_KAFKA_VERSION))
	if err != nil {
		failf("failed to read kafka version err=%v", err)
	}

	readAuthFile(args.auth, os.Getenv(ENV_AUTH), &cmd.auth)

	switch args.partitions {
	case "", "all":
		cmd.partitions = []int32{}
	default:
		pss := strings.Split(args.partitions, ",")
		for _, ps := range pss {
			p, err := strconv.ParseInt(ps, 10, 32)
			if err != nil {
				failf("partition id invalid err=%v", err)
			}
			cmd.partitions = append(cmd.partitions, int32(p))
		}
	}

	if cmd.partitions == nil {
		failf(`failed to interpret partitions flag %#v. Should be a comma separated list of partitions or "all".`, args.partitions)
	}

	if cmd.filterGroups, err = regexp.Compile(args.filterGroups); err != nil {
		failf("groups filter regexp invalid err=%v", err)
	}

	if cmd.filterTopics, err = regexp.Compile(args.filterTopics); err != nil {
		failf("topics filter regexp invalid err=%v", err)
	}

	if args.reset != "" && (args.topic == "" || args.group == "") {
		failf("group and topic are required to reset offsets.")
	}

	switch args.reset {
	case "newest":
		cmd.reset = sarama.OffsetNewest
	case "oldest":
		cmd.reset = sarama.OffsetOldest
	case "":
		// optional flag
		cmd.reset = resetNotSpecified
	default:
		cmd.reset, err = strconv.ParseInt(args.reset, 10, 64)
		if err != nil {
			warnf("failed to parse set %#v err=%v", args.reset, err)
			cmd.failStartup(fmt.Sprintf(`set value %#v not valid. either newest, oldest or specific offset expected.`, args.reset))
		}
	}

	envBrokers := os.Getenv(ENV_BROKERS)
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
	topic        string
	brokers      string
	auth         string
	partitions   string
	group        string
	filterGroups string
	filterTopics string
	reset        string
	verbose      bool
	pretty       bool
	version      string
	offsets      bool
}

func (cmd *groupCmd) parseFlags(as []string) groupArgs {
	var args groupArgs
	flags := flag.NewFlagSet("group", flag.ContinueOnError)
	flags.StringVar(&args.topic, "topic", "", "Topic to consume (required).")
	flags.StringVar(&args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	flags.StringVar(&args.auth, "auth", "", fmt.Sprintf("Path to auth configuration file, can also be set via %s env variable", ENV_AUTH))
	flags.StringVar(&args.group, "group", "", "Consumer group name.")
	flags.StringVar(&args.filterGroups, "filter-groups", "", "Regex to filter groups.")
	flags.StringVar(&args.filterTopics, "filter-topics", "", "Regex to filter topics.")
	flags.StringVar(&args.reset, "reset", "", "Target offset to reset for consumer group (newest, oldest, or specific offset)")
	flags.BoolVar(&args.verbose, "verbose", false, "More verbose logging to stderr.")
	flags.BoolVar(&args.pretty, "pretty", true, "Control output pretty printing.")
	flags.StringVar(&args.version, "version", "", "Kafka protocol version")
	flags.StringVar(&args.partitions, "partitions", allPartitionsHuman, "comma separated list of partitions to limit offsets to, or all")
	flags.BoolVar(&args.offsets, "offsets", true, "Controls if offsets should be fetched (defaults to true)")

	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of group:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, groupDocString)
	}

	err := flags.Parse(as)
	if err != nil && strings.Contains(err.Error(), "flag: help requested") {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}

	return args
}

var groupDocString = fmt.Sprintf(`
The values for -topic and -brokers can also be set via environment variables %s and %s respectively.
The values supplied on the command line win over environment variable values.

The group command can be used to list groups, their offsets and lag and to reset a group's offset.

When an explicit offset hasn't been set yet, kt prints out the respective sarama constants, cf. https://godoc.org/github.com/Shopify/sarama#pkg-constants

To simply list all groups:

kt group

This is faster when not fetching offsets:

kt group -offsets=false

To filter by regex:

kt group -filter specials

To filter by topic:

kt group -topic fav-topic

To reset a consumer group's offset:

kt group -reset 23 -topic fav-topic -group specials -partitions 2

To reset a consumer group's offset for all partitions:

kt group -reset newest -topic fav-topic -group specials -partitions all
`, ENV_TOPIC, ENV_BROKERS)
