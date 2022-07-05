package utils

import (
	"encoding/json"
	"log"
	"os"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/trolliama/geolocation-tracking-system/simulator/core/route"
	"github.com/trolliama/geolocation-tracking-system/simulator/infra/kafka"
)

func Produce(msg *ckafka.Message) {
	producer := kafka.NewKafkaProducer()
	local_route := route.NewRoute("", "", nil)

	json.Unmarshal(msg.Value, &local_route)
	local_route.LoadPositions()

	positions, err := local_route.ExportJsonPositions()
	if err != nil {
		log.Println((err.Error()))

	}

	for _, position := range positions {
		err := kafka.Publish(position, os.Getenv("KAFKA_PRODUCER_TOPIC"), producer)
		if err != nil {
			log.Println((err.Error()))
		}
		time.Sleep(time.Millisecond * 500)
	}
}
