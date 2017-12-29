package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"fluux.io/kafka/auth"
	"github.com/Shopify/sarama"
)

// Default values
const (
	defaultHost  = "localhost:9092"
	defaultTopic = "mytopic"
)

func main() {
	var (
		host       string
		topic      string
		clientCert string
		clientKey  string
		caCert     string
	)
	// TODO Support writing file to stdout + support -o option.
	flag.StringVar(&host, "host", defaultHost, "Kafka host")
	flag.StringVar(&topic, "topic", defaultTopic, "Kafka topic")
	flag.StringVar(&clientCert, "clientCert", "", "Client Certificate")
	flag.StringVar(&clientKey, "clientKey", "", "Client Key")
	flag.StringVar(&caCert, "caCert", "", "CA Certificate")
	flag.Parse()

	pemFiles := auth.PemFiles{
		ClientCert: clientCert,
		ClientKey:  clientKey,
		CACert:     caCert,
	}
	conn, err := kafkaConnect(host, pemFiles)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Create file and open it for writing
	// TODO check if file exist already ?
	filename := fmt.Sprintf("%s.kdump", topic)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	partState, err := conn.getPartitions(topic)
	if err != nil {
		log.Fatalln(err)
	}

	// Create a buffered writer from the file
	bufferedWriter := bufio.NewWriter(file)

	for partID, offset := range partState {
		partitionConsumer, err := conn.consumer.ConsumePartition(topic, partID, sarama.OffsetOldest)
		if err != nil {
			log.Fatalln(err)
		}

		if err = savePartition(partitionConsumer, bufferedWriter, offset, signals); err != nil {
			log.Println(err)
		}

		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}
	if err = bufferedWriter.Flush(); err != nil {
		log.Println("flush error:", err)
	}
}

//=============================================================================
// Consumer

type kafkaConn struct {
	client   sarama.Client
	consumer sarama.Consumer
}

func kafkaConnect(host string, p auth.PemFiles) (kafkaConn, error) {
	var conn = kafkaConn{}

	tlsConfig, err := auth.NewTLSConfig(p)
	if err != nil {
		log.Fatal(err)
	}

	consumerConfig := sarama.NewConfig()
	consumerConfig.Net.TLS.Enable = true
	consumerConfig.Net.TLS.Config = tlsConfig

	client, err := sarama.NewClient([]string{host}, consumerConfig)
	if err != nil {
		return conn, fmt.Errorf("unable to create kafka client: %q", err)
	}
	conn.client = client
	conn.consumer, err = sarama.NewConsumerFromClient(client)
	return conn, err
}

func (c kafkaConn) Close() {
	c.consumer.Close()
	c.client.Close()
}

//=============================================================================
// Get and Save topic partitions

// PartState is mapping current partition offset to each partition.
type PartState map[int32]int64

// getPartitions returns a map containing all partitions for a given topic and matching partition
// current offset (newestOffset).
// We read current offsets for each partition on program start to know when to stop.
// It means we dump Kafka data until current partOffset. We do not catch up on newly added data on
// purpose, as if Kafka is constantly receiving data, we will never stop.
func (c kafkaConn) getPartitions(topic string) (PartState, error) {
	partState := make(PartState)

	partIDs, err := c.client.Partitions(topic)
	if err != nil {
		return partState, fmt.Errorf("cannot get number of partIDs: %s", err)
	}

	for _, pid := range partIDs {
		offset, err := c.client.GetOffset(topic, pid, sarama.OffsetNewest)
		if err != nil {
			return partState, fmt.Errorf("cannot read offset for partition %d: %s", pid, err)
		}

		partState[pid] = offset
	}
	return partState, nil
}

func savePartition(consumer sarama.PartitionConsumer, buf *bufio.Writer, maxOffset int64, signals chan os.Signal) error {
	if maxOffset == 0 {
		log.Println("Skipping empty partition")
		return nil
	}
	for {
		select {
		case msg, more := <-consumer.Messages():
			fmt.Printf("Processing offset %d <= %d\n", msg.Offset, maxOffset)
			if !more {
				return nil
			}
			// TODO Store topic name to make sure we can always replay in right topic even if file is renamed
			// TODO Store key in file to make reinject / replay more accurate
			// key := string(msg.Key)
			pbuf := msg.Value
			length := len(pbuf)
			log.Println("Saving message of size ", length)
			// Write bytes to buffer
			size := make([]byte, 4)
			binary.BigEndian.PutUint32(size, uint32(length))

			if _, err := buf.Write(size); err != nil {
				return err
			}

			if _, err := buf.Write(pbuf); err != nil {
				return err
			}

			if msg.Offset+1 >= maxOffset {
				return nil
			}

		case <-signals: // Received a SIGINT from OS
			return nil
		}
	}

}
