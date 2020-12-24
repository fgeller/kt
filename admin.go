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
	"time"

	"github.com/Shopify/sarama"
)

type adminCmd struct {
	brokers []string
	verbose bool
	version sarama.KafkaVersion
	timeout *time.Duration
	auth    authConfig

	createTopic  string
	topicDetail  *sarama.TopicDetail
	validateOnly bool
	deleteTopic  string

	admin sarama.ClusterAdmin
}

type adminArgs struct {
	brokers string
	verbose bool
	version string
	timeout string
	auth    string

	createTopic     string
	topicDetailPath string
	validateOnly    bool
	deleteTopic     string
}

func (cmd *adminCmd) parseArgs(as []string) {
	var (
		args = cmd.parseFlags(as)
	)

	cmd.verbose = args.verbose
	cmd.version = kafkaVersion(args.version, os.Getenv(ENV_VERSION))

	cmd.timeout = parseTimeout(os.Getenv(ENV_ADMIN_TIMEOUT))
	if args.timeout != "" {
		cmd.timeout = parseTimeout(args.timeout)
	}

	readAuthFile(args.auth, os.Getenv(ENV_AUTH), &cmd.auth)

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

	cmd.validateOnly = args.validateOnly
	cmd.createTopic = args.createTopic
	cmd.deleteTopic = args.deleteTopic

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

	} else if cmd.deleteTopic != "" {
		cmd.runDeleteTopic()
	} else {
		failf("need to supply at least one sub-command of: createtopic, deletetopic")
	}
}

func (cmd *adminCmd) runCreateTopic() {
	err := cmd.admin.CreateTopic(cmd.createTopic, cmd.topicDetail, cmd.validateOnly)
	if err != nil {
		failf("failed to create topic err=%v", err)
	}
}

func (cmd *adminCmd) runDeleteTopic() {
	err := cmd.admin.DeleteTopic(cmd.deleteTopic)
	if err != nil {
		failf("failed to delete topic err=%v", err)
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

	if cmd.timeout != nil {
		cfg.Admin.Timeout = *cmd.timeout
	}

	if err = setupAuth(cmd.auth, cfg); err != nil {
		failf("failed to setup auth err=%v", err)
	}

	return cfg
}

func (cmd *adminCmd) parseFlags(as []string) adminArgs {
	var args adminArgs
	flags := flag.NewFlagSet("consume", flag.ContinueOnError)
	flags.StringVar(&args.brokers, "brokers", "", "Comma separated list of brokers. Port defaults to 9092 when omitted (defaults to localhost:9092).")
	flags.BoolVar(&args.verbose, "verbose", false, "More verbose logging to stderr.")
	flags.StringVar(&args.version, "version", "", "Kafka protocol version")
	flags.StringVar(&args.timeout, "timeout", "", "Timeout for request to Kafka (default: 3s)")
	flags.StringVar(&args.auth, "auth", "", fmt.Sprintf("Path to auth configuration file, can also be set via %s env variable", ENV_AUTH))

	flags.StringVar(&args.createTopic, "createtopic", "", "Name of the topic that should be created.")
	flags.StringVar(&args.topicDetailPath, "topicdetail", "", "Path to JSON encoded topic detail. cf sarama.TopicDetail")
	flags.BoolVar(&args.validateOnly, "validateonly", false, "Flag to indicate whether operation should only validate input (supported for createtopic).")

	flags.StringVar(&args.deleteTopic, "deletetopic", "", "Name of the topic that should be deleted.")

	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of admin:")
		flags.PrintDefaults()
		fmt.Fprintln(os.Stderr, adminDocString)
	}

	err := flags.Parse(as)
	if err != nil && strings.Contains(err.Error(), "flag: help requested") {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}

	return args
}

var adminDocString = fmt.Sprintf(`
The value for -brokers can also be set via environment variables %s.
The value supplied on the command line wins over the environment variable value.

If both -createtopic and deletetopic are supplied, -createtopic wins.

The topic details should be passed via a JSON file that represents a sarama.TopicDetail struct.
cf https://godoc.org/github.com/Shopify/sarama#TopicDetail

A simple way to pass a JSON file is to use a tool like https://github.com/fgeller/jsonify and shell's process substition:

kt admin -createtopic morenews -topicdetail <(jsonify =NumPartitions 1 =ReplicationFactor 1)`,
	ENV_BROKERS)
