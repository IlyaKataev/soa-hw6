package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"

	pb "warehouse/gen/warehouse/v1"
	"warehouse/internal/handler"
	"warehouse/internal/metrics"
	"warehouse/internal/sr"
)

const Topic = "warehouse-events"

type Consumer struct {
	consumer     *ckafka.Consumer
	dlqProducer  *ckafka.Producer
	deserializer *protobuf.Deserializer
	handler      *handler.Handler
	log          *slog.Logger
}

func NewConsumer(brokers, schemaRegistryURL string, handler *handler.Handler, log *slog.Logger) (*Consumer, error) {
	c, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           "warehouse-state-consumer",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, err
	}
	producer, err := ckafka.NewProducer(&ckafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		c.Close()
		return nil, err
	}
	srClient, err := schemaregistry.NewClient(sr.NewConfig(schemaRegistryURL))
	if err != nil {
		c.Close()
		producer.Close()
		return nil, err
	}
	deserializer, err := protobuf.NewDeserializer(srClient, serde.ValueSerde, protobuf.NewDeserializerConfig())
	if err != nil {
		c.Close()
		producer.Close()
		return nil, err
	}
	return &Consumer{consumer: c, dlqProducer: producer, deserializer: deserializer, handler: handler, log: log}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	if err := c.consumer.SubscribeTopics([]string{Topic}, nil); err != nil {
		return err
	}
	go metrics.TrackLag(ctx, c.consumer, Topic, 10*time.Second)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := c.consumer.ReadMessage(500 * time.Millisecond)
		if err != nil {
			if kafkaErr, ok := err.(ckafka.Error); ok && kafkaErr.Code() == ckafka.ErrTimedOut {
				continue
			}
			c.log.Error("kafka read failed", "error", err)
			continue
		}
		c.handleMessage(ctx, msg)
	}
}

func (c *Consumer) Close() {
	if c.consumer != nil {
		_ = c.consumer.Close()
	}
	if c.dlqProducer != nil {
		c.dlqProducer.Close()
	}
}

func (c *Consumer) Health(ctx context.Context) error {
	done := make(chan error, 1)
	go func() {
		_, err := c.consumer.GetMetadata(nil, false, 3000)
		done <- err
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func (c *Consumer) handleMessage(ctx context.Context, msg *ckafka.Message) {
	start := time.Now()
	var event pb.WarehouseEvent
	topic := Topic
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}
	if err := c.deserializer.DeserializeInto(topic, msg.Value, &event); err != nil {
		c.sendFailureToDLQAndCommit(msg, &event, fmt.Errorf("deserialize event: %w", err))
		return
	}

	err := c.handler.Handle(ctx, &event)
	metrics.EventProcessingDuration.WithLabelValues(event.EventType).Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.CassandraWriteErrorsTotal.Inc()
		c.sendFailureToDLQAndCommit(msg, &event, err)
		return
	}
	metrics.EventsProcessedTotal.WithLabelValues(event.EventType).Inc()
	if _, err := c.consumer.CommitMessage(msg); err != nil {
		c.log.Error("offset commit failed", "event_id", event.EventId, "event_type", event.EventType, "error", err)
		return
	}
	c.log.Info(
		"event processed",
		"event_id", event.EventId,
		"event_type", event.EventType,
		"offset", msg.TopicPartition.Offset,
		"partition", msg.TopicPartition.Partition,
	)
}

func (c *Consumer) sendFailureToDLQAndCommit(msg *ckafka.Message, event *pb.WarehouseEvent, processErr error) {
	c.log.Error(
		"event processing failed",
		"event_id", event.GetEventId(),
		"event_type", event.GetEventType(),
		"offset", msg.TopicPartition.Offset,
		"partition", msg.TopicPartition.Partition,
		"error", processErr,
	)
	if err := SendToDLQ(c.dlqProducer, event, processErr, msg.TopicPartition.Partition, msg.TopicPartition.Offset); err != nil {
		c.log.Error("send to dlq failed", "error", err)
		return
	}
	metrics.DLQEventsTotal.WithLabelValues(handler.ErrorCodeOf(processErr)).Inc()
	if _, err := c.consumer.CommitMessage(msg); err != nil {
		c.log.Error("offset commit after dlq failed", "error", err)
	}
}
