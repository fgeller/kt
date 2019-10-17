package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
)

type adminCmd struct {
	commonFlags
	timeout         time.Duration
	createTopic     string
	validateOnly    bool
	deleteTopic     string
	topicDetailPath string

	admin sarama.ClusterAdmin
}

func (cmd *adminCmd) run(args []string) error {
	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	cfg, err := cmd.saramaConfig("admin")
	if err != nil {
		return err
	}
	admin, err := sarama.NewClusterAdmin(cmd.brokers(), cfg)
	if err != nil {
		return fmt.Errorf("cannot create cluster admin: %v", err)
	}
	cmd.admin = admin

	switch {
	case cmd.createTopic != "":
		buf, err := ioutil.ReadFile(cmd.topicDetailPath)
		if err != nil {
			return err
		}
		var detail sarama.TopicDetail
		if err = json.Unmarshal(buf, &detail); err != nil {
			return fmt.Errorf("cannot unmarshal topic detail : %v", err)
		}
		if err := cmd.admin.CreateTopic(cmd.createTopic, &detail, cmd.validateOnly); err != nil {
			return fmt.Errorf("cannot create topic: %v", err)
		}
	case cmd.deleteTopic != "":
		err := cmd.admin.DeleteTopic(cmd.deleteTopic)
		if err != nil {
			return fmt.Errorf("cannot delete topic: %v", err)
		}
	default:
		return fmt.Errorf("need to supply at least one sub-command of: createtopic, deletetopic")
	}
	return nil
}

func (cmd *adminCmd) addFlags(flags *flag.FlagSet) {
	cmd.commonFlags.addFlags(flags)
	flags.DurationVar(&cmd.timeout, "timeout", 3*time.Second, "Timeout for request to Kafka")
	flags.StringVar(&cmd.createTopic, "createtopic", "", "Name of the topic that should be created.")
	flags.StringVar(&cmd.topicDetailPath, "topicdetail", "", "Path to JSON encoded topic detail. cf sarama.TopicDetail")
	flags.BoolVar(&cmd.validateOnly, "validateonly", false, "Flag to indicate whether operation should only validate input (supported for createtopic).")
	flags.StringVar(&cmd.deleteTopic, "deletetopic", "", "Name of the topic that should be deleted.")
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of admin:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, adminDocString)
	}
}

func (cmd *adminCmd) environFlags() map[string]string {
	return map[string]string{
		"timeout": "KT_ADMIN_TIMEOUT",
		"brokers": "KT_BROKERS",
	}
}

var adminDocString = `
The value for -brokers can also be set via environment variables KT_BROKERS.
The value supplied on the command line wins over the environment variable value.

If both -createtopic and deletetopic are supplied, -createtopic wins.

The topic details should be passed via a JSON file that represents a sarama.TopicDetail struct.
cf https://godoc.org/github.com/Shopify/sarama#TopicDetail

A simple way to pass a JSON file is to use a tool like https://github.com/fgeller/jsonify and shell's process substition:

kt admin -createtopic morenews -topicdetail <(jsonify =NumPartitions 1 =ReplicationFactor 1)`
