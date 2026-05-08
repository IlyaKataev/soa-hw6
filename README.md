# SOA HW6: Smart Warehouse

Event-driven система склада. Producer кладет события в Kafka, consumer обновляет Cassandra. Невалидные события уходят в DLQ. Метрики доступны через Prometheus/Grafana.

## Запуск

```bash
docker compose up --build -d
```

Producer запускается, когда надо отправить событие:

```bash
docker compose run --rm wms-producer \
  --event-type PRODUCT_RECEIVED \
  --product-id SKU-1 \
  --zone-id A1 \
  --quantity 100
```

- consumer health: http://localhost:8080/health
- метрики consumer: http://localhost:8080/metrics
- метрики Kafka exporter: http://localhost:9308/metrics
- Schema Registry: http://localhost:8081
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000

## Проверка

```bash
go test ./...
go vet ./...
docker compose config --quiet
buf lint
```

Полный прогон E2E-сценариев

```bash
docker compose up --build -d
docker compose build wms-producer
python3 scripts/e2e.py
```

## Protobuf

- текущая схема: `proto/warehouse/v1/events.proto`
- V1-схема для проверки совместимости: `migrations/events_v1.proto`. Это исторический снимок, из него Go-код не генерируется.
- сгенерированный Go-код: `gen/warehouse/v1/events.pb.go`

V1/V2 можно проверить руками:

```bash
docker compose run --rm wms-producer --schema-version 1 \
  --event-type PRODUCT_RECEIVED --product-id SKU-V1 --zone-id A1 --quantity 50

docker compose run --rm wms-producer --schema-version 2 \
  --event-type PRODUCT_RECEIVED --product-id SKU-V2 --zone-id A1 --quantity 50 \
  --supplier-id SUP-001
```

Schema Registry: сначала список версий, потом сами схемы. В V2 должно быть видно `optional string supplier_id = 4`.

```bash
curl -fsS http://localhost:8081/subjects/warehouse-events-value/versions
curl -fsS http://localhost:8081/subjects/warehouse-events-value/versions/1
curl -fsS http://localhost:8081/subjects/warehouse-events-value/versions/2
```

После изменения proto-схемы:

```bash
buf lint
buf generate
go test ./...
```

## Архитектура

```text
wms-producer -> Kafka warehouse-events -> warehouse-consumer -> Cassandra
                                            |
                                            v
                                      warehouse-events-dlq

warehouse-consumer -> /metrics        -> Prometheus -> Grafana
Kafka consumer group -> kafka-exporter -> Prometheus -> Grafana
```

- consumer group: `warehouse-state-consumer`
- offsets коммитятся после записи в Cassandra или подтвержденной отправки в DLQ
- lag для алерта считается через `kafka-exporter`, чтобы метрика была доступна даже при остановленном consumer
- Cassandra: 3 ноды, RF=3, бизнес-чтения и записи с `QUORUM`
- идемпотентность хранится в `processed_events`
- out-of-order события отсекаются через `last_event_timestamp`
- эволюция схемы: optional `supplier_id` в `ProductReceived`

## Полезные команды

```bash
# Cassandra ring
docker exec soa-hw6-cassandra-1 nodetool status

# consumer health
curl -fsS http://localhost:8080/health

# основные метрики
curl -fsS http://localhost:8080/metrics | grep -E '^(warehouse_events_processed|warehouse_consumer_lag|warehouse_cassandra_write)'

# lag из Kafka exporter
curl -fsS http://localhost:9308/metrics | grep '^kafka_consumergroup_lag'

# версии схемы warehouse-events-value
curl -fsS http://localhost:8081/subjects/warehouse-events-value/versions
```
