package kafka

import (
	"encoding/json"
	"fmt"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/encoding/protojson"

	pb "warehouse/gen/warehouse/v1"
	"warehouse/internal/handler"
)

const DLQTopic = "warehouse-events-dlq"

type DLQMessage struct {
	OriginalEvent any            `json:"original_event"`
	ErrorReason   string         `json:"error_reason"`
	ErrorCode     string         `json:"error_code"`
	FailedAt      string         `json:"failed_at"`
	KafkaMetadata map[string]any `json:"kafka_metadata"`
}

func SendToDLQ(producer *ckafka.Producer, original *pb.WarehouseEvent, err error, partition int32, offset ckafka.Offset) error {
	originalJSON := map[string]any{}
	if original != nil {
		raw, marshalErr := protojson.Marshal(original)
		if marshalErr == nil {
			_ = json.Unmarshal(raw, &originalJSON)
		}
	}
	message := DLQMessage{
		OriginalEvent: originalJSON,
		ErrorReason:   err.Error(),
		ErrorCode:     handler.ErrorCodeOf(err),
		FailedAt:      time.Now().UTC().Format(time.RFC3339Nano),
		KafkaMetadata: map[string]any{
			"partition": partition,
			"offset":    int64(offset),
		},
	}
	value, marshalErr := json.Marshal(message)
	if marshalErr != nil {
		return marshalErr
	}

	deliveryChan := make(chan ckafka.Event, 1)
	if produceErr := producer.Produce(&ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: new(DLQTopic), Partition: ckafka.PartitionAny},
		Value:          value,
	}, deliveryChan); produceErr != nil {
		return produceErr
	}

	select {
	case ev := <-deliveryChan:
		m, ok := ev.(*ckafka.Message)
		if !ok {
			return fmt.Errorf("unexpected DLQ delivery event %T", ev)
		}
		return m.TopicPartition.Error
	case <-time.After(10 * time.Second):
		return fmt.Errorf("DLQ delivery timed out")
	}
}
