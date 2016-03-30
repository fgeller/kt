package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type consumeConfig struct {
	topic       string
	brokers     []string
	startOffset int64
	endOffset   int64
	timeout     time.Duration
	args        struct {
		topic   string
		brokers string
		timeout time.Duration
		offsets string
	}
}

func print(msg *sarama.ConsumerMessage) {
	fmt.Printf(
		`{"partition":%v,"offset":%v,"key":%#v,"message":%#v}
`,
		msg.Partition,
		msg.Offset,
		string(msg.Key),
		string(msg.Value),
	)
}

func consumeCommand() command {
	consume := flag.NewFlagSet("consume", flag.ExitOnError)
	consume.StringVar(&config.consume.args.topic, "topic", "", "Topic to consume.")
	consume.StringVar(&config.consume.args.brokers, "brokers", "localhost:9092", "Comma separated list of brokers. Port defaults to 9092 when omitted.")
	consume.StringVar(&config.consume.args.offsets, "offsets", "", "Colon separated offsets where to start and end reading messages.")
	consume.DurationVar(&config.consume.timeout, "timeout", time.Duration(0), "Timeout after not reading messages (default 0 to disable).")

	consume.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of consume:")
		consume.PrintDefaults()
		os.Exit(2)
	}

	return command{
		flags: consume,
		parseArgs: func(args []string) {
			var err error

			if len(args) == 0 {
				consume.Usage()
			}

			consume.Parse(args)

			failStartup := func(msg string) {
				fmt.Fprintln(os.Stderr, msg)
				fmt.Fprintln(os.Stderr, "Use \"kt consume -help\" for more information.")
				os.Exit(1)
			}

			if config.consume.args.topic == "" {
				failStartup("Topic name is required.")
			}
			config.consume.topic = config.consume.args.topic

			config.consume.brokers = strings.Split(config.consume.args.brokers, ",")
			for i, b := range config.consume.brokers {
				if !strings.Contains(b, ":") {
					config.consume.brokers[i] = b + ":9092"
				}
			}

			offsets := strings.Split(config.consume.args.offsets, ":")

			switch {
			case len(offsets) > 2:
				failStartup(fmt.Sprintf("Invalid value for offsets: %v", offsets))
			case len(offsets) == 1 && len(offsets[0]) == 0:
				config.consume.startOffset = sarama.OffsetOldest
			case len(offsets) == 1:
				config.consume.startOffset, err = strconv.ParseInt(offsets[0], 10, 64)
				if err != nil {
					failStartup(fmt.Sprintf("Cannot parse start offset %v err=%v", offsets[0], err))
				}
			case len(offsets) == 2:
				if len(offsets[0]) == 0 {
					config.consume.startOffset = sarama.OffsetOldest
				} else {
					config.consume.startOffset, err = strconv.ParseInt(offsets[0], 10, 64)
					if err != nil {
						failStartup(fmt.Sprintf("Cannot parse start offset %v err=%v", offsets[0], err))
					}
				}

				if len(offsets[1]) == 0 {
					break
				}
				config.consume.endOffset, err = strconv.ParseInt(offsets[1], 10, 64)
				if err != nil {
					failStartup(fmt.Sprintf("Cannot parse end offset %v err=%v", offsets[1], err))
				}

				if config.consume.endOffset < config.consume.startOffset {
					failStartup(fmt.Sprintf("End offset cannot be less than start offset %v.", config.consume.startOffset))
				}
			}
		},

		run: func(closer chan struct{}) {

			consumer, err := sarama.NewConsumer(config.consume.brokers, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to create consumer err=%v\n", err)
				os.Exit(1)
			}

			partitions, err := consumer.Partitions(config.consume.topic)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to read partitions for topic %v err=%v\n", config.consume.topic, err)
				os.Exit(1)
			}

			var wg sync.WaitGroup
		consuming:
			for partition := range partitions {
				partitionConsumer, err := consumer.ConsumePartition(config.consume.topic, int32(partition), config.consume.startOffset)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to consume partition %v err=%v\n", partition, err)
					continue consuming
				}
				wg.Add(1)

				go func(pc sarama.PartitionConsumer, p int) {
					for {
						timeout := make(<-chan time.Time)
						if config.consume.timeout > 0 {
							timeout = time.After(config.consume.timeout)
						}

						select {
						case <-timeout:
							log.Printf("Consuming from partition [%v] timed out.", p)
							pc.Close()
							wg.Done()
							return
						case <-closer:
							pc.Close()
							wg.Done()
							return
						case msg, ok := <-pc.Messages():
							if ok {
								print(msg)
							}
							if config.consume.endOffset > 0 && msg.Offset >= config.consume.endOffset {
								pc.Close()
								wg.Done()
								return
							}
						}
					}
				}(partitionConsumer, partition)
			}

			wg.Wait()
			consumer.Close()
		},
	}
}
