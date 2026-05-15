package main

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	goproto "google.golang.org/protobuf/proto"

	pb "warehouse/gen/warehouse/v1"
	warehousekafka "warehouse/internal/kafka"
	"warehouse/internal/sr"
)

const schemaSubject = "warehouse-events-value"

func main() {
	var (
		brokers           = flag.String("brokers", getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"), "Kafka bootstrap servers")
		schemaRegistryURL = flag.String("schema-registry", getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"), "Schema Registry URL")
		schemaVersion     = flag.Int("schema-version", 2, "Schema Registry version to use: 1 or 2")
		eventID           = flag.String("event-id", "", "Event id. Generated when empty")
		eventType         = flag.String("event-type", "PRODUCT_RECEIVED", "Event type")
		timestamp         = flag.Int64("timestamp", 0, "Unix millis. Current time when empty")
		productID         = flag.String("product-id", "SKU-001", "Product id")
		zoneID            = flag.String("zone-id", "ZONE-A", "Zone id")
		quantity          = flag.Int("quantity", 1, "Quantity")
		supplierID        = flag.String("supplier-id", "", "Supplier id for PRODUCT_RECEIVED schema v2")
		fromZoneID        = flag.String("from-zone-id", "", "Source zone id for PRODUCT_MOVED")
		toZoneID          = flag.String("to-zone-id", "", "Destination zone id for PRODUCT_MOVED")
		orderID           = flag.String("order-id", "", "Order id")
		items             = flag.String("items", "", "Order items as product:zone:qty,product:zone:qty")
		count             = flag.Int("count", 1, "Number of events to publish")
	)
	flag.Parse()

	if *count < 1 {
		log.Fatal("--count must be positive")
	}
	if *schemaVersion != 1 && *schemaVersion != 2 {
		log.Fatal("--schema-version must be 1 or 2")
	}
	if *schemaVersion == 1 && *supplierID != "" {
		log.Fatal("--supplier-id requires --schema-version 2")
	}

	baseEventID := *eventID
	if baseEventID == "" {
		baseEventID = newEventID()
	}
	baseTimestamp := *timestamp
	if baseTimestamp == 0 {
		baseTimestamp = time.Now().UTC().UnixMilli()
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": *brokers})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	srClient, err := schemaregistry.NewClient(sr.NewConfig(*schemaRegistryURL))
	if err != nil {
		log.Fatal(err)
	}
	serializer, err := protobuf.NewSerializer(srClient, serde.ValueSerde, protobuf.NewSerializerConfig())
	if err != nil {
		log.Fatal(err)
	}

	delivery := make(chan kafka.Event, *count)
	defer close(delivery)

	publishedKey := ""
	for i := 0; i < *count; i++ {
		currentEventID := baseEventID
		if *count > 1 {
			currentEventID = fmt.Sprintf("%s-%d", baseEventID, i+1)
		}
		event, key, err := buildEvent(currentEventID, strings.ToUpper(*eventType), baseTimestamp+int64(i), *productID, *zoneID, *fromZoneID, *toZoneID, int32(*quantity), *orderID, *items, *supplierID)
		if err != nil {
			log.Fatal(err)
		}
		publishedKey = key
		value, err := serializeEvent(srClient, serializer, event, *schemaVersion)
		if err != nil {
			log.Fatal(err)
		}
		if err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: new(warehousekafka.Topic), Partition: kafka.PartitionAny},
			Key:            []byte(key),
			Value:          value,
		}, delivery); err != nil {
			log.Fatal(err)
		}
	}

	for i := 0; i < *count; i++ {
		select {
		case delivered := <-delivery:
			msg, ok := delivered.(*kafka.Message)
			if !ok {
				log.Fatalf("unexpected delivery event %T", delivered)
			}
			if msg.TopicPartition.Error != nil {
				log.Fatalf("delivery failed: %v", msg.TopicPartition.Error)
			}
		case <-time.After(30 * time.Second):
			log.Fatal("timed out waiting for delivery")
		}
	}
	fmt.Printf("published count=%d event_type=%s key=%s\n", *count, strings.ToUpper(*eventType), publishedKey)
}

func serializeEvent(srClient schemaregistry.Client, serializer *protobuf.Serializer, event *pb.WarehouseEvent, schemaVersion int) ([]byte, error) {
	if schemaVersion == 2 {
		return serializer.Serialize(warehousekafka.Topic, event)
	}

	metadata, err := srClient.GetSchemaMetadata(schemaSubject, schemaVersion)
	if err != nil {
		return nil, err
	}
	msgBytes, err := goproto.Marshal(event)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, 0, 6+len(msgBytes))
	payload = append(payload, 0)
	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, uint32(metadata.ID))
	payload = append(payload, idBytes...)
	payload = append(payload, 0)
	payload = append(payload, msgBytes...)
	return payload, nil
}

func buildEvent(eventID, eventType string, timestamp int64, productID, zoneID, fromZoneID, toZoneID string, quantity int32, orderID, rawItems, supplierID string) (*pb.WarehouseEvent, string, error) {
	event := &pb.WarehouseEvent{EventId: eventID, EventType: eventType, Timestamp: timestamp}
	key := productID
	switch eventType {
	case "PRODUCT_RECEIVED":
		payload := &pb.ProductReceived{ProductId: productID, ZoneId: zoneID, Quantity: quantity}
		if supplierID != "" {
			payload.SupplierId = &supplierID
		}
		event.Payload = &pb.WarehouseEvent_ProductReceived{ProductReceived: payload}
	case "PRODUCT_SHIPPED":
		event.Payload = &pb.WarehouseEvent_ProductShipped{ProductShipped: &pb.ProductShipped{ProductId: productID, ZoneId: zoneID, Quantity: quantity}}
	case "PRODUCT_MOVED":
		if fromZoneID == "" {
			fromZoneID = zoneID
		}
		if toZoneID == "" {
			return nil, "", fmt.Errorf("--to-zone-id is required for PRODUCT_MOVED")
		}
		event.Payload = &pb.WarehouseEvent_ProductMoved{ProductMoved: &pb.ProductMoved{ProductId: productID, FromZoneId: fromZoneID, ToZoneId: toZoneID, Quantity: quantity}}
	case "PRODUCT_RESERVED":
		event.Payload = &pb.WarehouseEvent_ProductReserved{ProductReserved: &pb.ProductReserved{ProductId: productID, ZoneId: zoneID, Quantity: quantity, OrderId: orderID}}
	case "PRODUCT_RELEASED":
		event.Payload = &pb.WarehouseEvent_ProductReleased{ProductReleased: &pb.ProductReleased{ProductId: productID, ZoneId: zoneID, Quantity: quantity, OrderId: orderID}}
	case "INVENTORY_COUNTED":
		event.Payload = &pb.WarehouseEvent_InventoryCounted{InventoryCounted: &pb.InventoryCounted{ProductId: productID, ZoneId: zoneID, CountedQuantity: quantity}}
	case "ORDER_CREATED":
		if orderID == "" {
			orderID = "ORD-" + eventID
		}
		items, err := parseItems(rawItems, productID, zoneID, quantity)
		if err != nil {
			return nil, "", err
		}
		event.Payload = &pb.WarehouseEvent_OrderCreated{OrderCreated: &pb.OrderCreated{OrderId: orderID, Items: items}}
		key = orderID
	case "ORDER_COMPLETED":
		if orderID == "" {
			return nil, "", fmt.Errorf("--order-id is required for ORDER_COMPLETED")
		}
		event.Payload = &pb.WarehouseEvent_OrderCompleted{OrderCompleted: &pb.OrderCompleted{OrderId: orderID}}
		key = orderID
	default:
		return nil, "", fmt.Errorf("unknown --event-type %q", eventType)
	}
	return event, key, nil
}

func parseItems(raw, productID, zoneID string, quantity int32) ([]*pb.OrderItem, error) {
	if strings.TrimSpace(raw) == "" {
		return []*pb.OrderItem{{ProductId: productID, ZoneId: zoneID, Quantity: quantity}}, nil
	}
	parts := strings.Split(raw, ",")
	result := make([]*pb.OrderItem, 0, len(parts))
	for _, part := range parts {
		fields := strings.Split(strings.TrimSpace(part), ":")
		if len(fields) != 3 {
			return nil, fmt.Errorf("invalid item %q, expected product:zone:qty", part)
		}
		qty, err := strconv.Atoi(fields[2])
		if err != nil {
			return nil, fmt.Errorf("invalid quantity in item %q: %w", part, err)
		}
		result = append(result, &pb.OrderItem{ProductId: fields[0], ZoneId: fields[1], Quantity: int32(qty)})
	}
	return result, nil
}

func newEventID() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return "evt-" + hex.EncodeToString(buf[:])
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
