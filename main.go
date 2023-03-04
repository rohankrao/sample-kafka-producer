package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"io"
	"net/http"
	"os"
)

// VectorDemoLogs is a demo logs record from Vector
type VectorDemoLogs struct {
	//ContainerCreatedAt string            `json:"container_created_at"`
	//ContainerId        string            `json:"container_id"`
	//ContainerName      string            `json:"container_name"`
	//Host               string            `json:"host"`
	//Stream             string            `json:"stream"`
	//Label              map[string]string `json:"label"`

	Message    string `json:"message"`
	SourceType string `json:"source_type"`
	Timestamp  string `json:"timestamp"`
}

type RecordsHandler struct {
	producer *kafka.Producer
	ser      *avro.GenericSerializer
	topic    string
}

func (rh *RecordsHandler) produce(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		fmt.Printf("could not read body: %s\n", err)
	}

	var records []VectorDemoLogs

	err = json.Unmarshal(body, &records)
	if err != nil {
		fmt.Printf("could not unmarshal body: %s\n", err)
		return
	}

	for _, record := range records {
		payload, err := rh.ser.Serialize(rh.topic, &record)
		if err != nil {
			fmt.Printf("Failed to serialize payload: %s\n", err)
			continue
		}

		err = rh.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &rh.topic, Partition: kafka.PartitionAny},
			Value:          payload,
		}, nil)
		if err != nil {
			fmt.Printf("Produce failed: %v\n", err)
		}
	}

	_, err = io.WriteString(w, "Success!\n")
	if err != nil {
		fmt.Printf("could not write: %s\n", err)
		return
	}

}

func main() {
	bootstrapServers := "192.168.1.6:9092"
	url := "http://192.168.1.6:30081"
	topic := "test-go-demo-logs"

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)

	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(url))

	if err != nil {
		fmt.Printf("Failed to create schema registry client: %s\n", err)
		os.Exit(1)
	}

	ser, err := avro.NewGenericSerializer(client, serde.ValueSerde, avro.NewSerializerConfig())

	myRecordsHandler := &RecordsHandler{
		p,
		ser,
		topic,
	}

	if err != nil {
		fmt.Printf("Failed to create serializer: %s\n", err)
		os.Exit(1)
	}

	http.HandleFunc("/produce", myRecordsHandler.produce)

	err = http.ListenAndServe(":3333", nil)
	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}

}
