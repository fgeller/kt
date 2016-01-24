package main

import (
	"log"
	"os"
	"os/signal"
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
	log.Printf(
		"Partition=%v Offset=%v Key=%s Value=%s\n",
		msg.Partition,
		msg.Offset,
		msg.Key,
		msg.Value,
	)
}

func main() {
	topic := "fg-test"
	brokers := []string{"localhost:9092"}

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
		partitionConsumer, err := consumer.ConsumePartition(topic, int32(partition), 10)
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
				}
			}
		}(partitionConsumer)
	}
	wg.Wait()
	consumer.Close()
}
