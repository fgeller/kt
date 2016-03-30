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

type interval struct {
	start int64
	end   int64
}

type consumeConfig struct {
	topic   string
	brokers []string
	offsets map[int32]interval
	timeout time.Duration
	args    struct {
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

func parseOffsets(str string) (map[int32]interval, error) {
	result := map[int32]interval{}

	partitions := strings.Split(str, ",")
	for _, partition := range partitions {
		if len(partition) == 0 {
			fmt.Printf("Skipping empty partition [%s]\n", partition)
			continue
		}

		partition = strings.TrimSuffix(partition, ":")
		// 0
		// 0:
		if !strings.Contains(partition, ":") {
			p, err := strconv.Atoi(partition)
			if err != nil {
				return result, fmt.Errorf("Invalid offsets definition: %s.", partition)
			}
			result[int32(p)] = interval{}
			continue
		}

		if strings.Count(partition, ":") != 1 ||
			strings.Count(partition, "-") > 1 {
			return result, fmt.Errorf("Invalid offsets definition: %s.", partition)
		}

		// 0:1
		// 0:1-
		// 0:1-2
		// 0:-2
		p, err := strconv.Atoi(partition[:strings.Index(partition, ":")])
		if err != nil {
			return result, fmt.Errorf("Invalid offsets definition: %s.", partition)
		}

		var i interval
		start := partition[strings.Index(partition, ":")+1:]
		end := ""
		if strings.Contains(start, "-") {
			end = start[strings.Index(start, "-")+1:]
			start = start[:strings.Index(start, "-")]
		}

		if len(start) > 0 {
			s, err := strconv.Atoi(start)
			if err != nil {
				return result, fmt.Errorf("Invalid offsets definition: %s.", partition)
			}
			i.start = int64(s)
		}

		if len(end) > 0 {
			e, err := strconv.Atoi(end)
			if err != nil {
				return result, fmt.Errorf("Invalid offsets definition: %s.", partition)
			}
			i.end = int64(e)
		}

		result[int32(p)] = i
	}

	return result, nil
}

func consumeCommand() command {
	consume := flag.NewFlagSet("consume", flag.ExitOnError)
	consume.StringVar(&config.consume.args.topic, "topic", "", "Topic to consume.")
	consume.StringVar(&config.consume.args.brokers, "brokers", "localhost:9092", "Comma separated list of brokers. Port defaults to 9092 when omitted.")
	consume.StringVar(&config.consume.args.offsets, "offsets", "", "Specifies what messages to read by partition and offset range.")
	consume.DurationVar(&config.consume.timeout, "timeout", time.Duration(0), "Timeout after not reading messages (default 0 to disable).")

	consume.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of consume:")
		consume.PrintDefaults()
		fmt.Fprintln(os.Stderr, ` `) // TODO
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

			config.consume.offsets, err = parseOffsets(config.consume.args.offsets)
			if err != nil {
				failStartup(fmt.Sprintf("%s", err))
			}
		},

		run: func(closer chan struct{}) {

			consumer, err := sarama.NewConsumer(config.consume.brokers, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to create consumer err=%v\n", err)
				os.Exit(1)
			}

			allPartitions, err := consumer.Partitions(config.consume.topic)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to read partitions for topic %v err=%v\n", config.consume.topic, err)
				os.Exit(1)
			}

			partitions := []int32{}
			if len(config.consume.offsets) > 0 {
				for _, p := range partitions {
					_, ok := config.consume.offsets[p]
					if ok {
						partitions = append(partitions, p)
					}
				}
			} else {
				partitions = allPartitions
			}

			var wg sync.WaitGroup
		consuming:
			for _, partition := range partitions {
				offsets, ok := config.consume.offsets[partition]
				if !ok {
					offsets = interval{0, 0}
				}
				partitionConsumer, err := consumer.ConsumePartition(
					config.consume.topic,
					int32(partition),
					offsets.start,
				)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to consume partition %v err=%v\n", partition, err)
					continue consuming
				}
				wg.Add(1)

				go func(pc sarama.PartitionConsumer, p int32) {
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
							if offsets.end > 0 && msg.Offset >= offsets.end {
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
