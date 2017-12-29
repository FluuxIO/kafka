package main

// Run with:
// go build examples/consumer/*.go
// ./consumer -host mykafkahost.net:9093

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"

	"fluux.io/kafka/auth"
	"github.com/Shopify/sarama"
)

// Default values
const (
	defaultHost  = "localhost:9093"
	defaultTopic = "test"

	defaultClientCert = "bundle/client.cer.pem"
	defaultClientKey  = "bundle/client.key.pem"
	defaultCACert     = "bundle/server.cer.pem"
)

func main() {
	var (
		host       string
		topic      string
		clientCert string
		clientKey  string
		caCert     string
	)

	flag.StringVar(&host, "host", defaultHost, "Kafka host")
	flag.StringVar(&topic, "topic", defaultTopic, "Kafka topic")
	flag.StringVar(&clientCert, "clientCert", defaultClientCert, "Client Certificate")
	flag.StringVar(&clientKey, "clientKey", defaultClientKey, "Client Key")
	flag.StringVar(&caCert, "caCert", defaultCACert, "CA Certificate")
	flag.Parse()

	pemFiles := auth.PemFiles{
		ClientCert: clientCert,
		ClientKey:  clientKey,
		CACert:     caCert,
	}

	consumer, err := consumerSetup(host, pemFiles)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	consumerLoop(consumer, topic)
}

//=============================================================================
// Consumer

func consumerSetup(host string, p auth.PemFiles) (sarama.Consumer, error) {
	tlsConfig, err := auth.NewTLSConfig(p)
	if err != nil {
		log.Fatal(err)
	}
	// This can be used on test server if domain does not match cert:
	// tlsConfig.InsecureSkipVerify = true

	consumerConfig := sarama.NewConfig()
	consumerConfig.Net.TLS.Enable = true
	consumerConfig.Net.TLS.Config = tlsConfig

	client, err := sarama.NewClient([]string{host}, consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create kafka client: %q", err)
	}

	return sarama.NewConsumerFromClient(client)
}

func consumerLoop(consumer sarama.Consumer, topic string) {
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Println("unable to fetch partition IDs for the topic", topic, err)
		return
	}

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var wg sync.WaitGroup
	for partition := range partitions {
		wg.Add(1)
		go func() {
			consumePartition(consumer, topic, int32(partition), signals)
			wg.Done()
		}()
	}
	wg.Wait()
}

func consumePartition(consumer sarama.Consumer, topic string, partition int32, signals chan os.Signal) {
	log.Println("Receving on partition", partition)
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Println(err)
		return
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Println(err)
		}
	}()

	consumed := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Consumed message offset %d\nData: %s\n", msg.Offset, msg.Value)
			consumed++
		case <-signals:
			break ConsumerLoop
		}
	}
	log.Printf("Consumed: %d\n", consumed)
}
