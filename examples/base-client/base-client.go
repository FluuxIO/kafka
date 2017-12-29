package main

// Run with:
// go build examples/base-client/*.go
// ./base-client

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/Shopify/sarama"
)

func main() {
	tlsConfig, err := NewTLSConfig("bundle/client.cer.pem",
		"bundle/client.key.pem",
		"bundle/server.cer.pem")
	if err != nil {
		log.Fatal(err)
	}
	// This can be used on test server if domain does not match cert:
	// tlsConfig.InsecureSkipVerify = true

	consumerConfig := sarama.NewConfig()
	consumerConfig.Net.TLS.Enable = true
	consumerConfig.Net.TLS.Config = tlsConfig

	client, err := sarama.NewClient([]string{"localhost:9093"}, consumerConfig)
	if err != nil {
		log.Fatalf("unable to create kafka client: %q", err)
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	consumerLoop(consumer, "test")
}

// NewTLSConfig generates a TLS configuration used to authenticate on server with
// certificates.
// Parameters are the three pem files path we need to authenticate: client cert, client key and CA cert.
func NewTLSConfig(clientCertFile, clientKeyFile, caCertFile string) (*tls.Config, error) {
	tlsConfig := tls.Config{}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		return &tlsConfig, err
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	// Load CA cert
	caCert, err := ioutil.ReadFile(caCertFile)
	if err != nil {
		return &tlsConfig, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	tlsConfig.BuildNameToCertificate()
	return &tlsConfig, err
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
			consumePartition(consumer, int32(partition), signals)
			wg.Done()
		}()
	}
	wg.Wait()
}

func consumePartition(consumer sarama.Consumer, partition int32, signals chan os.Signal) {
	log.Println("Receving on partition", partition)
	partitionConsumer, err := consumer.ConsumePartition("test", partition, sarama.OffsetNewest)
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
