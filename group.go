package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/Shopify/sarama"
)

type groupCmd struct {
	commonFlags
	group           string
	topic           string
	filterGroupsStr string
	filterTopicsStr string
	partitionsStr   string
	resetStr        string
	verbose         bool
	pretty          bool
	version         sarama.KafkaVersion
	offsets         bool

	reset        int64
	filterGroups *regexp.Regexp
	filterTopics *regexp.Regexp
	partitions   []int32
	client       sarama.Client
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

func (cmd *groupCmd) addFlags(flags *flag.FlagSet) {
	cmd.commonFlags.addFlags(flags)
	flags.StringVar(&cmd.topic, "topic", "", "Topic to consume (required).")
	flags.StringVar(&cmd.group, "group", "", "Consumer group name.")
	flags.StringVar(&cmd.filterGroupsStr, "filter-groups", "", "Regex to filter groups.")
	flags.StringVar(&cmd.filterTopicsStr, "filter-topics", "", "Regex to filter topics.")
	flags.StringVar(&cmd.resetStr, "reset", "", "Target offset to reset for consumer group (newest, oldest, or specific offset)")
	flags.BoolVar(&cmd.pretty, "pretty", true, "Control output pretty printing.")
	flags.StringVar(&cmd.partitionsStr, "partitions", allPartitionsHuman, "comma separated list of partitions to limit offsets to, or all")
	flags.BoolVar(&cmd.offsets, "offsets", true, "Controls if offsets should be fetched (defaults to true)")

	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of group:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, groupDocString)
	}
}

func (cmd *groupCmd) environFlags() map[string]string {
	return map[string]string{
		"topic":   "KT_TOPIC",
		"brokers": "KT_BROKERS",
	}
}

func (cmd *groupCmd) run(args []string) error {
	if err := cmd.init(); err != nil {
		return err
	}
	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	cfg, err := cmd.saramaConfig("group")
	if err != nil {
		return err
	}
	if cmd.client, err = sarama.NewClient(cmd.brokers, cfg); err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}

	brokers := cmd.client.Brokers()
	fmt.Fprintf(os.Stderr, "found %v brokers\n", len(brokers))

	groups := []string{cmd.group}
	if cmd.group == "" {
		groups = []string{}
		allGroups, err := cmd.findGroups(brokers)
		if err != nil {
			return err
		}
		for _, g := range allGroups {
			if cmd.filterGroups.MatchString(g) {
				groups = append(groups, g)
			}
		}
	}
	fmt.Fprintf(os.Stderr, "found %v groups\n", len(groups))

	topics := []string{cmd.topic}
	if cmd.topic == "" {
		allTopics, err := cmd.client.Topics()
		if err != nil {
			return fmt.Errorf("failed to read topics: %v", err)
		}
		topics = []string{}
		for _, t := range allTopics {
			if cmd.filterTopics.MatchString(t) {
				topics = append(topics, t)
			}
		}
	}
	fmt.Fprintf(os.Stderr, "found %v topics\n", len(topics))

	out := newPrinter(cmd.pretty)
	if !cmd.offsets {
		for _, grp := range groups {
			out.print(group{Name: grp})
		}
		return nil
	}

	topicPartitions := map[string][]int32{}
	for _, topic := range topics {
		parts := cmd.partitions
		if len(parts) == 0 {
			parts, err = cmd.client.Partitions(topic)
			if err != nil {
				return fmt.Errorf("failed to read partitions for topic=%s: %v", topic, err)
			}
			fmt.Fprintf(os.Stderr, "found partitions=%v for topic=%v\n", parts, topic)
		}
		topicPartitions[topic] = parts
	}

	var wg errgroup.Group
	for _, grp := range groups {
		for top, parts := range topicPartitions {
			grp, top, parts := grp, top, parts
			wg.Go(func() error {
				return cmd.printGroupTopicOffset(out, grp, top, parts)
			})
		}
	}
	return wg.Wait()
}

func (cmd *groupCmd) init() error {
	switch cmd.partitionsStr {
	case "", "all":
		cmd.partitions = []int32{}
	default:
		pss := strings.Split(cmd.partitionsStr, ",")
		for _, ps := range pss {
			p, err := strconv.ParseInt(ps, 10, 32)
			if err != nil {
				return fmt.Errorf("partition id invalid: %v", err)
			}
			cmd.partitions = append(cmd.partitions, int32(p))
		}
	}

	if cmd.partitions == nil {
		return fmt.Errorf(`failed to interpret partitions flag %#v. Should be a comma separated list of partitions or "all".`, cmd.partitions)
	}

	var err error
	if cmd.filterGroups, err = regexp.Compile(cmd.filterGroupsStr); err != nil {
		return fmt.Errorf("groups filter regexp invalid: %v", err)
	}

	if cmd.filterTopics, err = regexp.Compile(cmd.filterTopicsStr); err != nil {
		return fmt.Errorf("topics filter regexp invalid: %v", err)
	}

	if cmd.resetStr != "" && (cmd.topic == "" || cmd.group == "") {
		return fmt.Errorf("group and topic are required to reset offsets.")
	}

	switch cmd.resetStr {
	case "newest":
		cmd.reset = sarama.OffsetNewest
	case "oldest":
		cmd.reset = sarama.OffsetOldest
	case "":
		// optional flag
		cmd.reset = resetNotSpecified
	default:
		cmd.reset, err = strconv.ParseInt(cmd.resetStr, 10, 64)
		if err != nil {
			return fmt.Errorf(`set value %#v not valid. either newest, oldest or specific offset expected.`, cmd.resetStr)
		}
	}
	return nil
}

func (cmd *groupCmd) printGroupTopicOffset(out *printer, grp, top string, parts []int32) error {
	target := group{
		Name:    grp,
		Topic:   top,
		Offsets: make([]groupOffset, 0, len(parts)),
	}
	results := make(chan groupOffset)
	var wg errgroup.Group
	for _, part := range parts {
		part := part
		wg.Go(func() error {
			return cmd.fetchGroupOffset(grp, top, part, results)
		})
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	for res := range results {
		target.Offsets = append(target.Offsets, res)
	}
	if err := wg.Wait(); err != nil {
		return err
	}
	if len(target.Offsets) > 0 {
		sort.Slice(target.Offsets, func(i, j int) bool {
			return target.Offsets[j].Partition > target.Offsets[i].Partition
		})
		out.print(target)
	}
	return nil
}

func (cmd *groupCmd) resolveOffset(top string, part int32, off int64) (int64, error) {
	resolvedOff, err := cmd.client.GetOffset(top, part, off)
	if err != nil {
		return 0, fmt.Errorf("failed to get offset to reset to for partition=%d: %v", part, err)
	}

	if cmd.verbose {
		fmt.Fprintf(os.Stderr, "resolved offset %v for topic=%s partition=%d to %v\n", off, top, part, resolvedOff)
	}

	return resolvedOff, nil
}

func (cmd *groupCmd) fetchGroupOffset(grp, top string, part int32, results chan<- groupOffset) error {
	if cmd.verbose {
		fmt.Fprintf(os.Stderr, "fetching offset information for group=%v topic=%v partition=%v\n", grp, top, part)
	}

	offsetManager, err := sarama.NewOffsetManagerFromClient(grp, cmd.client)
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}
	defer logClose("offset manager", offsetManager)

	pom, err := offsetManager.ManagePartition(top, part)
	if err != nil {
		return fmt.Errorf("failed to manage partition group=%s topic=%s partition=%d: %v", grp, top, part, err)
	}
	defer logClose("partition offset manager", pom)

	specialOffset := cmd.reset < 0

	groupOff, _ := pom.NextOffset()
	if cmd.reset >= 0 || specialOffset {
		resolvedOff := cmd.reset
		if specialOffset {
			resolvedOff, err = cmd.resolveOffset(top, part, cmd.reset)
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
		return nil
	}

	partOff, err := cmd.resolveOffset(top, part, sarama.OffsetNewest)
	if err != nil {
		return err
	}
	lag := partOff - groupOff
	results <- groupOffset{Partition: part, Offset: &groupOff, Lag: &lag}
	return nil
}

type findGroupResult struct {
	done  bool
	group string
}

func (cmd *groupCmd) findGroups(brokers []*sarama.Broker) ([]string, error) {
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
			return groups, nil
		}

		select {
		case err := <-errs:
			return nil, fmt.Errorf("failed to find groups: %v", err)
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
		errs <- fmt.Errorf("failed to connect to broker %#v: %v", broker.Addr(), err)
	}

	if resp, err = broker.ListGroups(&sarama.ListGroupsRequest{}); err != nil {
		errs <- fmt.Errorf("failed to list brokers on %#v: %v", broker.Addr(), err)
	}

	if resp.Err != sarama.ErrNoError {
		errs <- fmt.Errorf("failed to list brokers on %#v: %v", broker.Addr(), resp.Err)
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

	cfg, err := cmd.saramaConfig("group")
	if err != nil {
		return err
	}
	if err := broker.Open(cfg); err != nil {
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

var groupDocString = `
The values for -topic and -brokers can also be set via environment variables KT_TOPIC and KT_BROKERS respectively.
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
`
