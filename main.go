package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
)

var config struct {
	topic       string
	brokers     []string
	startOffset int64
	endOffset   int64
	jsonOutput  bool
}

func listenForInterrupt() chan struct{} {
	closer := make(chan struct{})
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		log.Printf("Received interrupt - shutting down...")
		close(closer)
	}()

	return closer
}

func print(msg *sarama.ConsumerMessage) {

	if config.jsonOutput {
		fmt.Printf(
			`{"partition":%v,"offset":%v,"key":%#v,"message":%#v}
`,
			msg.Partition,
			msg.Offset,
			string(msg.Key),
			string(msg.Value),
		)

		return
	}

	fmt.Printf(
		"Partition=%v Offset=%v Key=%s Message=%s\n",
		msg.Partition,
		msg.Offset,
		msg.Key,
		msg.Value,
	)
}

func parseArgs() {
	var (
		err           error
		brokersString string
		offset        string
	)

	failStartup := func(msg string) {
		fmt.Fprintln(os.Stderr, msg)
		flag.Usage()
		os.Exit(1)
	}

	flag.StringVar(&config.topic, "topic", "", "Topic to consume.")
	flag.StringVar(&brokersString, "brokers", "localhost:9092", "Comma separated list of brokers. Port defaults to 9092 when omitted.")
	flag.StringVar(&offset, "offset", "", "Colon separated offsets where to start and end reading messages.")
	flag.BoolVar(&config.jsonOutput, "json", false, "Print output in JSON format.")

	flag.Parse()

	if config.topic == "" {
		failStartup("Topic name is required.")
	}

	config.brokers = strings.Split(brokersString, ",")

	for i, b := range config.brokers {
		if !strings.Contains(b, ":") {
			config.brokers[i] = b + ":9092"
		}
	}

	offsets := strings.Split(offset, ":")

	switch {
	case len(offsets) > 2:
		failStartup(fmt.Sprintf("Invalid value for offsets: %v", offsets))
	case len(offsets) == 1 && len(offsets[0]) == 0:
		config.startOffset = sarama.OffsetOldest
	case len(offsets) == 1:
		config.startOffset, err = strconv.ParseInt(offsets[0], 10, 64)
		if err != nil {
			failStartup(fmt.Sprintf("1 Cannot parse start offset %v err=%v", offsets[0], err))
		}
	case len(offsets) == 2:
		if len(offsets[0]) == 0 {
			config.startOffset = sarama.OffsetOldest
		} else {
			config.startOffset, err = strconv.ParseInt(offsets[0], 10, 64)
			if err != nil {
				failStartup(fmt.Sprintf("2 Cannot parse start offset %v err=%v", offsets[0], err))
			}
		}

		if len(offsets[1]) == 0 {
			break
		}
		config.endOffset, err = strconv.ParseInt(offsets[1], 10, 64)
		if err != nil {
			failStartup(fmt.Sprintf("Cannot parse end offset %v err=%v", offsets[1], err))
		}

		if config.endOffset < config.startOffset {
			failStartup(fmt.Sprintf("End offset cannot be less than start offset %v.", config.startOffset))
		}
	}
}

func main() {
	parseArgs()

	closer := listenForInterrupt()

	consumer, err := sarama.NewConsumer(config.brokers, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer err=%v\n", err)
		os.Exit(1)
	}

	partitions, err := consumer.Partitions(config.topic)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read partitions for topic %v err=%v\n", config.topic, err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
consuming:
	for partition := range partitions {
		partitionConsumer, err := consumer.ConsumePartition(config.topic, int32(partition), config.startOffset)
		if err != nil {
			log.Printf("Failed to consume partition %v err=%v\n", partition, err)
			continue consuming
		}
		wg.Add(1)

		go func(pc sarama.PartitionConsumer) {
			for {
				select {
				case <-closer:
					pc.Close()
					wg.Done()
					return
				case msg, ok := <-pc.Messages():
					if ok {
						print(msg)
					}
					if config.endOffset > 0 && msg.Offset >= config.endOffset {
						pc.Close()
						wg.Done()
						return
					}
				}
			}
		}(partitionConsumer)
	}
	wg.Wait()
	consumer.Close()
}
