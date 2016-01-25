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
	fmt.Printf(
		"Partition=%v Offset=%v Key=%s Value=%s\n",
		msg.Partition,
		msg.Offset,
		msg.Key,
		msg.Value,
	)
}

func main() {
	var (
		err           error
		topic         string
		brokers       []string
		brokersString string
		offset        string
		startOffset   int64
		endOffset     int64
	)

	flag.StringVar(&topic, "topic", "", "Topic to consume.")
	flag.StringVar(&brokersString, "brokers", "localhost:9092", "Comma separated list of brokers.")
	flag.StringVar(&offset, "offset", "", "Colon separated offsets where to start and end reading messages.")

	flag.Parse()

	if topic == "" {
		log.Fatalf("Topic name is required.\n")
	}
	brokers = strings.Split(brokersString, ",")
	offsets := strings.Split(offset, ":")

	switch {
	case len(offsets) > 2:
		log.Fatalf("Invalid value for offsets: %v\n")
	case len(offsets) == 0:
		startOffset = sarama.OffsetOldest
	case len(offsets) == 1:
		startOffset, err = strconv.ParseInt(offsets[0], 10, 64)
		if err != nil {
			log.Fatalf("Cannot parse start offset %v err=%v", offsets[0], err)
		}
	case len(offsets) == 2:
		startOffset, err = strconv.ParseInt(offsets[0], 10, 64)
		if err != nil {
			log.Fatalf("Cannot parse start offset %v err=%v", offsets[0], err)
		}
		if len(offsets[1]) == 0 {
			break
		}
		endOffset, err = strconv.ParseInt(offsets[1], 10, 64)
		if err != nil {
			log.Fatalf("Cannot parse end offset %v err=%v", offsets[1], err)
		}
		if endOffset <= startOffset {
			log.Fatalf("End offset cannot be less than start offset %v.", startOffset)
		}
	}

	closer := listenForInterrupt()

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v\n", err)
	}

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatalf("Failed to read partitions for topic %v err=%v\n", topic, err)
	}

	var wg sync.WaitGroup
consuming:
	for partition := range partitions {
		partitionConsumer, err := consumer.ConsumePartition(topic, int32(partition), startOffset)
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
					if endOffset > 0 && msg.Offset >= endOffset {
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
