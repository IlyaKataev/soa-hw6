package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"warehouse/internal/cassandra"
	"warehouse/internal/handler"
	warehousekafka "warehouse/internal/kafka"
	"warehouse/internal/metrics"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	session, err := cassandra.NewSession(cassandra.HostsFromEnv(), getenv("CASSANDRA_DC", "datacenter1"))
	if err != nil {
		log.Error("connect cassandra failed", "error", err)
		os.Exit(1)
	}
	store := cassandra.NewStore(session)
	defer store.Close()

	metrics.Register()
	consumerHandler := handler.New(store, log)
	consumer, err := warehousekafka.NewConsumer(
		getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
		getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
		consumerHandler,
		log,
	)
	if err != nil {
		log.Error("create kafka consumer failed", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	healthServer := metrics.Serve(getenv("HTTP_ADDR", ":8080"), func(w http.ResponseWriter, r *http.Request) {
		healthCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err := errors.Join(store.Health(healthCtx), consumer.Health(healthCtx)); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "degraded", "reason": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = healthServer.Shutdown(shutdownCtx)
	}()

	if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("consumer stopped", "error", err)
		os.Exit(1)
	}
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
