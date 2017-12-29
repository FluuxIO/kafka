package main

// Run with:
// go build examples/producer/*.go
// ./producer -host mykafkahost.net:9093

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

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

func init() {
	rand.Seed(time.Now().UnixNano())
}

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

	producer, err := producerSetup(host, pemFiles)
	if err != nil {
		log.Fatal(err)
	}

	producerLoop(producer, topic)

	if err := producer.Close(); err != nil {
		log.Fatalln(err)
	}
}

//=============================================================================
// Producer behaviour

func producerSetup(host string, p auth.PemFiles) (sarama.AsyncProducer, error) {
	tlsConfig, err := auth.NewTLSConfig(p)
	if err != nil {
		log.Fatal(err)
	}
	// This can be used on test server if domain does not match cert:
	// tlsConfig.InsecureSkipVerify = true

	producerConfig := sarama.NewConfig()
	producerConfig.Net.TLS.Enable = true
	producerConfig.Net.TLS.Config = tlsConfig

	client, err := sarama.NewClient([]string{host}, producerConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create kafka client: %q", err)
	}

	return sarama.NewAsyncProducerFromClient(client)
}

func producerLoop(producer sarama.AsyncProducer, topic string) {
	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var enqueued, errors int
ProducerLoop:
	for {
		select {
		case <-signals:
			break ProducerLoop
		case <-time.After(time.Second * 1):
		}

		select {
		case producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: getRandomValue(10)}:
			log.Printf("Produced message %d\n", enqueued)
			enqueued++
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
			errors++
		}
	}
	log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
}

//=============================================================================
// Produce random string

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func getRandomValue(n int) sarama.StringEncoder {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return sarama.StringEncoder(b)
}
