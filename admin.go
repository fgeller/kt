package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"strings"

	"github.com/Shopify/sarama"
)

type adminCmd struct {
	brokers []string
	verbose bool
	version sarama.KafkaVersion

	createTopic  string
	topicDetail  *sarama.TopicDetail
	validateOnly bool

	admin sarama.ClusterAdmin
}

type adminArgs struct {
	brokers string
	verbose bool
	version string

	createTopic     string
	topicDetailPath string
	validateOnly    bool
}

// TODO tls
// TODO read detail from stdin?
// kt admin -createtopic name -topicdetail path -validateonly false
func (cmd *adminCmd) parseArgs(as []string) {
	var (
		args = cmd.parseFlags(as)
	)

	cmd.verbose = args.verbose
	cmd.version = kafkaVersion(args.version)

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

	cmd.validateOnly = args.validateOnly
	cmd.createTopic = args.createTopic

	if cmd.createTopic != "" {
		buf, err := ioutil.ReadFile(args.topicDetailPath)
		if err != nil {
			failf("failed to read topic detail err=%v", err)
		}

		var detail sarama.TopicDetail
		if err = json.Unmarshal(buf, &detail); err != nil {
			failf("failed to unmarshal topic detail err=%v", err)
		}
		cmd.topicDetail = &detail
	}
}

func (cmd *adminCmd) run(args []string) {
	var err error

	cmd.parseArgs(args)

	if cmd.verbose {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	if cmd.admin, err = sarama.NewClusterAdmin(cmd.brokers, cmd.saramaConfig()); err != nil {
		failf("failed to create cluster admin err=%v", err)
	}

	if cmd.createTopic != "" {
		cmd.runCreateTopic()
	} else {
		failf("need to supply at least one sub-command of: createtopic")
	}
}

func (cmd *adminCmd) runCreateTopic() {
	err := cmd.admin.CreateTopic(cmd.createTopic, cmd.topicDetail, cmd.validateOnly)
	if err != nil {
		failf("failed to create topic err=%v", err)
	}
}

func (cmd *adminCmd) saramaConfig() *sarama.Config {
	var (
		err error
		usr *user.User
		cfg = sarama.NewConfig()
	)

	cfg.Version = cmd.version
	if usr, err = user.Current(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-admin-" + sanitizeUsername(usr.Username)

	return cfg
}

func (cmd *adminCmd) parseFlags(as []string) adminArgs {
	var args adminArgs
	flags := flag.NewFlagSet("consume", flag.ExitOnError)
	flags.StringVar(&args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	flags.BoolVar(&args.verbose, "verbose", false, "More verbose logging to stderr.")
	flags.StringVar(&args.version, "version", "", "Kafka protocol version")

	flags.StringVar(&args.createTopic, "createtopic", "", "Name of the topic that should be created.")
	flags.StringVar(&args.topicDetailPath, "topicdetail", "", "Path to JSON encoded topic detail. cf sarama.TopicDetail")
	flags.BoolVar(&args.validateOnly, "validateonly", false, "Flag to indicate whether operation should only validate input (supported for createtopic).")

	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of admin:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, adminDocString)
		os.Exit(2)
	}

	flags.Parse(as)
	return args
}

var adminDocString = `TODO`
