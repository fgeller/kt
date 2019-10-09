package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"

	"github.com/Shopify/sarama"
	"golang.org/x/sync/errgroup"
)

type topicCmd struct {
	commonFlags
	partitions bool
	leaders    bool
	replicas   bool
	pretty     bool
	filterStr  string

	filter *regexp.Regexp
	client sarama.Client
}

type topic struct {
	Name       string      `json:"name"`
	Partitions []partition `json:"partitions,omitempty"`
}

type partition struct {
	Id           int32   `json:"id"`
	OldestOffset int64   `json:"oldest"`
	NewestOffset int64   `json:"newest"`
	Leader       string  `json:"leader,omitempty"`
	Replicas     []int32 `json:"replicas,omitempty"`
	ISRs         []int32 `json:"isrs,omitempty"`
}

func (cmd *topicCmd) addFlags(flags *flag.FlagSet) {
	cmd.commonFlags.addFlags(flags)
	flags.BoolVar(&cmd.partitions, "partitions", false, "Include information per partition.")
	flags.BoolVar(&cmd.leaders, "leaders", false, "Include leader information per partition.")
	flags.BoolVar(&cmd.replicas, "replicas", false, "Include replica ids per partition.")
	flags.StringVar(&cmd.filterStr, "filter", "", "Regex to filter topics by name.")
	flags.BoolVar(&cmd.pretty, "pretty", true, "Control output pretty printing.")
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of topic:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, topicDocString)
	}
}

func (cmd *topicCmd) environFlags() map[string]string {
	return map[string]string{
		"brokers": "KT_BROKERS",
	}
}

func (cmd *topicCmd) run(as []string) error {
	var err error
	cmd.filter, err = regexp.Compile(cmd.filterStr)
	if err != nil {
		return fmt.Errorf("invalid regex for filter: %v", err)
	}
	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	if err := cmd.connect(); err != nil {
		return err
	}
	defer cmd.client.Close()

	all, err := cmd.client.Topics()
	if err != nil {
		return fmt.Errorf("failed to read topics: %v", err)
	}

	topics := []string{}
	for _, a := range all {
		if cmd.filter.MatchString(a) {
			topics = append(topics, a)
		}
	}

	out := newPrinter(cmd.pretty)
	var wg errgroup.Group
	for _, topicName := range topics {
		topicName := topicName
		wg.Go(func() error {
			topic, err := cmd.readTopic(topicName)
			if err != nil {
				return fmt.Errorf("failed to read info for topic %s: %v", topicName, err)
			}
			out.print(topic)
			return nil
		})
	}
	return wg.Wait()
}

func (cmd *topicCmd) connect() error {
	cfg, err := cmd.saramaConfig("topic")
	if err != nil {
		return err
	}
	if cmd.client, err = sarama.NewClient(cmd.brokers, cfg); err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}
	return nil
}

func (cmd *topicCmd) readTopic(name string) (topic, error) {
	var (
		err error
		ps  []int32
		led *sarama.Broker
		top = topic{Name: name}
	)

	if !cmd.partitions {
		return top, nil
	}

	if ps, err = cmd.client.Partitions(name); err != nil {
		return top, err
	}

	for _, p := range ps {
		np := partition{Id: p}

		if np.OldestOffset, err = cmd.client.GetOffset(name, p, sarama.OffsetOldest); err != nil {
			return top, err
		}

		if np.NewestOffset, err = cmd.client.GetOffset(name, p, sarama.OffsetNewest); err != nil {
			return top, err
		}

		if cmd.leaders {
			if led, err = cmd.client.Leader(name, p); err != nil {
				return top, err
			}
			np.Leader = led.Addr()
		}

		if cmd.replicas {
			if np.Replicas, err = cmd.client.Replicas(name, p); err != nil {
				return top, err
			}

			if np.ISRs, err = cmd.client.InSyncReplicas(name, p); err != nil {
				return top, err
			}
		}

		top.Partitions = append(top.Partitions, np)
	}

	return top, nil
}

var topicDocString = `
The values for -brokers can also be set via the environment variable KT_BROKERS respectively.
The values supplied on the command line win over environment variable values.`
