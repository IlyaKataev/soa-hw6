# Demo

Шпаргалка для ручного показа. Перед каждым новым прогоном задай новый `DEMO_ID`, чтобы команды не конфликтовали со старыми строками в Cassandra, Kafka offsets и DLQ.

```bash
export DEMO_ID=$(date +%Y%m%d%H%M%S)
echo "DEMO_ID=${DEMO_ID}"
```

Старт окружения:

```bash
docker compose up -d
docker compose build wms-producer
```

Health check:

```bash
docker compose ps
curl -fsS http://localhost:8080/health
docker exec soa-hw6-cassandra-1 nodetool status
```

## 1. Базовый цикл

```bash
docker compose run --rm wms-producer \
  --event-type PRODUCT_RECEIVED \
  --product-id SKU-DEMO-1-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 100

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON product_id, zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-1-${DEMO_ID}' AND zone_id='ZONE-A';"

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON product_id, total_available, total_reserved FROM warehouse.inventory_totals WHERE product_id='SKU-DEMO-1-${DEMO_ID}';"

docker compose run --rm wms-producer \
  --event-type PRODUCT_RESERVED \
  --product-id SKU-DEMO-1-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 30 \
  --order-id ORD-RES-DEMO-1-${DEMO_ID}

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON product_id, zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-1-${DEMO_ID}' AND zone_id='ZONE-A';"

docker compose run --rm wms-producer \
  --event-type PRODUCT_MOVED \
  --product-id SKU-DEMO-1-${DEMO_ID} \
  --from-zone-id ZONE-A \
  --to-zone-id ZONE-B \
  --quantity 20

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON product_id, zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-1-${DEMO_ID}';"

docker compose run --rm wms-producer \
  --event-type PRODUCT_SHIPPED \
  --product-id SKU-DEMO-1-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 10

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON product_id, zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-1-${DEMO_ID}' AND zone_id='ZONE-A';"

docker compose run --rm wms-producer \
  --event-type ORDER_CREATED \
  --product-id SKU-DEMO-1-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 15 \
  --order-id ORD-DEMO-1-${DEMO_ID}

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON product_id, zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-1-${DEMO_ID}' AND zone_id='ZONE-A';"

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON order_id, status FROM warehouse.orders WHERE order_id='ORD-DEMO-1-${DEMO_ID}';"

docker compose run --rm wms-producer \
  --event-type ORDER_COMPLETED \
  --order-id ORD-DEMO-1-${DEMO_ID}

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON product_id, zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-1-${DEMO_ID}';"

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON order_id, status FROM warehouse.orders WHERE order_id='ORD-DEMO-1-${DEMO_ID}';"
```

Ожидаемые состояния по шагам:

- после `PRODUCT_RECEIVED`: `ZONE-A available=100 reserved=0`, `inventory_totals total_available=100 total_reserved=0`;
- после `PRODUCT_RESERVED`: `ZONE-A available=70 reserved=30`;
- после `PRODUCT_MOVED`: `ZONE-A available=50 reserved=30`, `ZONE-B available=20 reserved=0`;
- после `PRODUCT_SHIPPED`: `ZONE-A available=40 reserved=30`;
- после `ORDER_CREATED`: `ZONE-A available=25 reserved=45`, заказ `CREATED`;
- после `ORDER_COMPLETED`: `ZONE-A available=25 reserved=30`, `ZONE-B available=20 reserved=0`, заказ `COMPLETED`.

## 2. Идемпотентность

```bash
docker compose run --rm wms-producer \
  --event-id evt-demo-idempotent-${DEMO_ID} \
  --event-type PRODUCT_RECEIVED \
  --product-id SKU-DEMO-2-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 50

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-2-${DEMO_ID}' AND zone_id='ZONE-A';"

docker compose run --rm wms-producer \
  --event-id evt-demo-idempotent-${DEMO_ID} \
  --event-type PRODUCT_RECEIVED \
  --product-id SKU-DEMO-2-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 50

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-2-${DEMO_ID}' AND zone_id='ZONE-A';"
```

Ожидаемый результат: `available_quantity = 50`, не `100`.

## 3. Консистентность таблиц

```bash
docker compose run --rm wms-producer \
  --event-type PRODUCT_RECEIVED \
  --product-id SKU-DEMO-3-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 100

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-3-${DEMO_ID}' AND zone_id='ZONE-A';"

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON zone_id, product_id, available_quantity FROM warehouse.inventory_by_zone WHERE zone_id='ZONE-A' AND product_id='SKU-DEMO-3-${DEMO_ID}';"

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON product_id, total_available FROM warehouse.inventory_totals WHERE product_id='SKU-DEMO-3-${DEMO_ID}';"
```

Во всех трех таблицах должно быть `100`. Это показывает, что batch обновил денормализованные представления вместе.

## 4. Out-of-order

```bash
NOW_MS=$(date +%s000)
T1=$((NOW_MS - 300000))
T2=$NOW_MS
T3=$((NOW_MS - 180000))

docker compose run --rm wms-producer \
  --event-id evt-demo-oor-1-${DEMO_ID} \
  --timestamp "$T1" \
  --event-type PRODUCT_RECEIVED \
  --product-id SKU-DEMO-4-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 100

docker compose run --rm wms-producer \
  --event-id evt-demo-oor-2-${DEMO_ID} \
  --timestamp "$T2" \
  --event-type PRODUCT_SHIPPED \
  --product-id SKU-DEMO-4-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 20

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-4-${DEMO_ID}' AND zone_id='ZONE-A';"

docker compose run --rm wms-producer \
  --event-id evt-demo-oor-3-${DEMO_ID} \
  --timestamp "$T3" \
  --event-type PRODUCT_RECEIVED \
  --product-id SKU-DEMO-4-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 50

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-4-${DEMO_ID}' AND zone_id='ZONE-A';"
```

Должно остаться `available_quantity = 80`. Третье событие старее второго, поэтому consumer его пропускает.

## 5. DLQ

```bash
docker compose run --rm wms-producer \
  --event-id evt-demo-dlq-${DEMO_ID} \
  --event-type PRODUCT_SHIPPED \
  --product-id SKU-DEMO-BAD-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity -5

docker exec -i soa-hw6-kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic warehouse-events-dlq \
  --from-beginning \
  --timeout-ms 5000

curl -fsS http://localhost:8080/health

docker compose run --rm wms-producer \
  --event-id evt-demo-after-dlq-${DEMO_ID} \
  --event-type PRODUCT_RECEIVED \
  --product-id SKU-DEMO-AFTER-DLQ-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 7

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-AFTER-DLQ-${DEMO_ID}' AND zone_id='ZONE-A';"
```

В DLQ должен быть `evt-demo-dlq-${DEMO_ID}`. Команда читает topic с начала, так что там могут быть и старые сообщения от прошлых прогонов. `/health` должен вернуть `ok`, а валидное событие после ошибки должно записаться в Cassandra с `available_quantity = 7`.

## 6. Cassandra failover

```bash
docker exec soa-hw6-cassandra-1 nodetool status

docker compose run --rm wms-producer \
  --event-type PRODUCT_RECEIVED \
  --product-id SKU-DEMO-6-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 200

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-6-${DEMO_ID}' AND zone_id='ZONE-A';"

docker stop soa-hw6-cassandra-2

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "CONSISTENCY ONE; SELECT JSON product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-6-${DEMO_ID}' AND zone_id='ZONE-A';"

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "CONSISTENCY QUORUM; SELECT JSON product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-6-${DEMO_ID}' AND zone_id='ZONE-A';"

# Ожидаемо завершится ошибкой, пока одна из трех реплик недоступна.
docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "CONSISTENCY ALL; SELECT JSON product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-6-${DEMO_ID}' AND zone_id='ZONE-A';"

docker compose run --rm wms-producer \
  --event-type PRODUCT_SHIPPED \
  --product-id SKU-DEMO-6-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 50

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-6-${DEMO_ID}' AND zone_id='ZONE-A';"

docker start soa-hw6-cassandra-2
sleep 10
docker exec soa-hw6-cassandra-1 nodetool status
```

После первого события должно быть `available_quantity = 200`. При остановленной одной Cassandra-ноде `ONE` и `QUORUM` должны читать данные, `ALL` ожидаемо падает, а запись consumer'а с `QUORUM` должна пройти: финально `available_quantity = 150`. После `docker start` в `nodetool status` снова ждем 3 `UN`.

## 7. Monitoring и lag

```bash
curl -fsS http://localhost:8080/health

docker compose run --rm wms-producer \
  --event-id evt-demo-metrics-burst-${DEMO_ID} \
  --event-type PRODUCT_RECEIVED \
  --product-id SKU-DEMO-METRICS-BURST-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 1 \
  --count 10

docker compose run --rm wms-producer \
  --event-id evt-demo-metrics-received-${DEMO_ID} \
  --event-type PRODUCT_RECEIVED \
  --product-id SKU-DEMO-METRICS-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 20

docker compose run --rm wms-producer \
  --event-id evt-demo-metrics-reserved-${DEMO_ID} \
  --event-type PRODUCT_RESERVED \
  --product-id SKU-DEMO-METRICS-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 5 \
  --order-id ORD-DEMO-METRICS-RESERVE-${DEMO_ID}

docker compose run --rm wms-producer \
  --event-id evt-demo-metrics-released-${DEMO_ID} \
  --event-type PRODUCT_RELEASED \
  --product-id SKU-DEMO-METRICS-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 2 \
  --order-id ORD-DEMO-METRICS-RESERVE-${DEMO_ID}

sleep 3
curl -fsS http://localhost:8080/metrics | grep -E '^(warehouse_events_processed|warehouse_consumer_lag|warehouse_dlq_events)'
curl -fsS http://localhost:9308/metrics | grep '^kafka_consumergroup_lag'
```

- Grafana: http://localhost:3000
- Prometheus alerts: http://localhost:9090/alerts

Проверка lag alert:

```bash
docker stop soa-hw6-warehouse-consumer

docker compose run --rm wms-producer \
  --event-id evt-demo-lag-${DEMO_ID} \
  --event-type PRODUCT_RECEIVED \
  --product-id SKU-DEMO-LAG-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 1 \
  --count 120

docker exec -i soa-hw6-kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group warehouse-state-consumer

curl -fsS http://localhost:9308/metrics | grep '^kafka_consumergroup_lag'
sleep 70
curl -fsS http://localhost:9090/api/v1/alerts | grep -E 'WarehouseConsumerLagHigh|firing'
```

Через минуту `WarehouseConsumerLagHigh` должен стать `firing`, alert rule имеет `for: 1m`.

Вернуть consumer:

```bash
docker start soa-hw6-warehouse-consumer
sleep 10

docker exec -i soa-hw6-kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --describe \
  --group warehouse-state-consumer
```

Lag должен вернуться к `0`, когда consumer догонит backlog.

## 8. Schema Evolution

```bash
docker compose run --rm wms-producer --schema-version 1 \
  --event-type PRODUCT_RECEIVED \
  --product-id SKU-DEMO-V1-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 50

docker compose run --rm wms-producer --schema-version 2 \
  --event-type PRODUCT_RECEIVED \
  --product-id SKU-DEMO-V2-${DEMO_ID} \
  --zone-id ZONE-A \
  --quantity 50 \
  --supplier-id SUP-001

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON product_id, zone_id, available_quantity, supplier_id FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-V1-${DEMO_ID}' AND zone_id='ZONE-A';"

docker exec -i soa-hw6-cassandra-1 cqlsh -e \
  "SELECT JSON product_id, zone_id, available_quantity, supplier_id FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-DEMO-V2-${DEMO_ID}' AND zone_id='ZONE-A';"

curl -fsS http://localhost:8081/subjects/warehouse-events-value/versions
curl -fsS http://localhost:8081/subjects/warehouse-events-value/versions/1
curl -fsS http://localhost:8081/subjects/warehouse-events-value/versions/2
curl -fsS http://localhost:8081/config
```

V1 хранит `supplier_id = null`, V2 хранит `supplier_id = "SUP-001"`. В Schema Registry у subject должны быть версии `1` и `2`, а в `/config` — `BACKWARD`.
