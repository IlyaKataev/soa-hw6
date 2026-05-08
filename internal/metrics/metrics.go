package metrics

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	ConsumerLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "warehouse_consumer_lag", Help: "Kafka consumer lag by partition."},
		[]string{"partition"},
	)
	EventsProcessedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "warehouse_events_processed_total", Help: "Processed warehouse events."},
		[]string{"event_type"},
	)
	EventProcessingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "warehouse_event_processing_duration_seconds",
			Help:    "Warehouse event processing duration.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"event_type"},
	)
	CassandraWriteErrorsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{Name: "warehouse_cassandra_write_errors_total", Help: "Cassandra write errors."},
	)
	DLQEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "warehouse_dlq_events_total", Help: "Events sent to DLQ."},
		[]string{"error_code"},
	)
)

func Register() {
	prometheus.MustRegister(ConsumerLag, EventsProcessedTotal, EventProcessingDuration, CassandraWriteErrorsTotal, DLQEventsTotal)
}

func Serve(addr string, health http.HandlerFunc) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", health)
	server := &http.Server{Addr: addr, Handler: mux}
	go func() {
		_ = server.ListenAndServe()
	}()
	return server
}

func TrackLag(ctx context.Context, consumer *kafka.Consumer, topic string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			assignment, err := consumer.Assignment()
			if err != nil || len(assignment) == 0 {
				continue
			}
			partitions, err := consumer.Committed(assignment, 5000)
			if err != nil {
				continue
			}
			for _, partition := range partitions {
				if partition.Partition < 0 {
					continue
				}
				_, high, err := consumer.QueryWatermarkOffsets(topic, partition.Partition, 5000)
				if err != nil {
					continue
				}
				committed := max(int64(partition.Offset), 0)
				lag := max(int64(high)-committed, 0)
				ConsumerLag.WithLabelValues(strconv.Itoa(int(partition.Partition))).Set(float64(lag))
			}
		}
	}
}
