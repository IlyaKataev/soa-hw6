import argparse
import json
import re
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone


CQLSH = ["docker", "exec", "-i", "soa-hw6-cassandra-1", "cqlsh"]
PRODUCER = ["docker", "compose", "run", "--rm", "wms-producer"]
KAFKA = ["docker", "exec", "-i", "soa-hw6-kafka"]
SR_URL = "http://localhost:8081"
CONSUMER_URL = "http://localhost:8080"
PROMETHEUS_URL = "http://localhost:9090"
KAFKA_EXPORTER_URL = "http://localhost:9308"


class E2EError(Exception):
    pass


def cmd_text(args):
    return shlex.join(str(arg) for arg in args)


def run_cmd(args, *, check=True, echo=True, timeout=None):
    if echo:
        print(f"> {cmd_text(args)}", flush=True)
    proc = subprocess.run(
        args,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )
    if echo and proc.stdout:
        print(proc.stdout, end="" if proc.stdout.endswith("\n") else "\n")
    if echo and proc.stderr:
        print(proc.stderr, end="" if proc.stderr.endswith("\n") else "\n", file=sys.stderr)
    if check and proc.returncode != 0:
        raise E2EError(f"command failed with exit code {proc.returncode}: {cmd_text(args)}")
    return proc


def section(title):
    print()
    print("--------------------------------")
    print(f"  {title}")
    print("--------------------------------")
    print()


def ok(message):
    print(f"OK {message}", flush=True)


def fail(message):
    raise E2EError(message)


def wait_until(description, predicate, *, timeout=30, interval=1):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                ok(description)
                return result
        except Exception as exc:
            last_error = exc
        time.sleep(interval)
    if last_error:
        fail(f"timeout waiting for {description}: {last_error}")
    fail(f"timeout waiting for {description}")


def cql_quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def cql_json(query):
    proc = run_cmd(CQLSH + ["-e", query], echo=False)
    rows = []
    for line in proc.stdout.splitlines():
        stripped = line.strip()
        if stripped.startswith("{") and stripped.endswith("}"):
            rows.append(json.loads(stripped))
    return rows


def select_one_json(columns, table, where):
    query = f"SELECT JSON {', '.join(columns)} FROM {table} WHERE {where};"
    rows = cql_json(query)
    if len(rows) != 1:
        raise AssertionError(f"expected 1 row, got {len(rows)} for query: {query}")
    return rows[0]


def assert_row(columns, table, where, expected, label, *, timeout=30):
    last_row = None

    def matches():
        nonlocal last_row
        last_row = select_one_json(columns, table, where)
        return all(last_row.get(key) == value for key, value in expected.items())

    try:
        wait_until(label, matches, timeout=timeout)
    except E2EError as exc:
        raise E2EError(f"{exc}; last row={last_row!r}, expected={expected!r}") from exc


def assert_rows(columns, table, where, expected_rows, label, *, timeout=30):
    query = f"SELECT JSON {', '.join(columns)} FROM {table} WHERE {where};"
    last_rows = None

    def matches():
        nonlocal last_rows
        last_rows = cql_json(query)
        normalized = sorted(last_rows, key=lambda row: json.dumps(row, sort_keys=True))
        expected = sorted(expected_rows, key=lambda row: json.dumps(row, sort_keys=True))
        return normalized == expected

    try:
        wait_until(label, matches, timeout=timeout)
    except E2EError as exc:
        raise E2EError(f"{exc}; last rows={last_rows!r}, expected={expected_rows!r}") from exc


def http_get(url, *, timeout=5):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8")


def http_json(url):
    return json.loads(http_get(url))


def read_metrics():
    return http_get(f"{CONSUMER_URL}/metrics")


def read_kafka_exporter_metrics():
    return http_get(f"{KAFKA_EXPORTER_URL}/metrics")


def prometheus_query(query):
    params = urllib.parse.urlencode({"query": query})
    result = http_json(f"{PROMETHEUS_URL}/api/v1/query?{params}")
    if result.get("status") != "success":
        fail(f"Prometheus query failed: {result!r}")
    return result.get("data", {}).get("result", [])


def metric_values(metrics_text, metric_name):
    values = []
    for line in metrics_text.splitlines():
        if line.startswith("#") or not (line.startswith(metric_name + "{") or line.startswith(metric_name + " ")):
            continue
        parts = line.rsplit(" ", 1)
        if len(parts) == 2:
            values.append(float(parts[1]))
    return values


def consumer_group_lag():
    proc = run_cmd(
        KAFKA
        + [
            "kafka-consumer-groups",
            "--bootstrap-server",
            "kafka:9092",
            "--describe",
            "--group",
            "warehouse-state-consumer",
        ],
        echo=False,
        timeout=30,
    )
    total = 0
    for line in proc.stdout.splitlines():
        parts = line.split()
        if len(parts) < 6 or parts[0] != "warehouse-state-consumer" or parts[1] != "warehouse-events":
            continue
        if parts[5].isdigit():
            total += int(parts[5])
    return total


def assert_prometheus_alert_rule():
    rules = http_json(f"{PROMETHEUS_URL}/api/v1/rules")
    if rules.get("status") != "success":
        fail(f"Prometheus rules API failed: {rules!r}")
    for group in rules.get("data", {}).get("groups", []):
        for rule in group.get("rules", []):
            if rule.get("name") == "WarehouseConsumerLagHigh":
                if "kafka_consumergroup_lag" not in rule.get("query", ""):
                    fail(f"unexpected lag alert expression: {rule!r}")
                ok("Prometheus lag alert rule is loaded")
                return
    fail("Prometheus lag alert rule is missing")


def alert_state(alert_name):
    alerts = http_json(f"{PROMETHEUS_URL}/api/v1/alerts")
    if alerts.get("status") != "success":
        fail(f"Prometheus alerts API failed: {alerts!r}")
    for alert in alerts.get("data", {}).get("alerts", []):
        labels = alert.get("labels") or {}
        if labels.get("alertname") == alert_name:
            return alert.get("state")
    return ""


def produce(**kwargs):
    args = PRODUCER[:]
    for key, value in kwargs.items():
        flag = "--" + key.replace("_", "-")
        args.extend([flag, str(value)])
    run_cmd(args, timeout=120)


def assert_dlq_event(event_id, error_code):
    deadline = time.monotonic() + 25
    last_messages = []
    while time.monotonic() < deadline:
        proc = run_cmd(
            KAFKA
            + [
                "kafka-console-consumer",
                "--bootstrap-server",
                "kafka:9092",
                "--topic",
                "warehouse-events-dlq",
                "--from-beginning",
                "--timeout-ms",
                "5000",
            ],
            check=False,
            echo=False,
            timeout=15,
        )
        last_messages = []
        for line in proc.stdout.splitlines():
            stripped = line.strip()
            if not stripped.startswith("{"):
                continue
            try:
                message = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            last_messages.append(message)
            original_event = message.get("original_event") or {}
            if original_event.get("eventId") == event_id:
                if message.get("error_code") != error_code:
                    fail(f"DLQ event {event_id} has error_code={message.get('error_code')!r}, expected {error_code!r}")
                metadata = message.get("kafka_metadata") or {}
                if "partition" not in metadata or "offset" not in metadata:
                    fail(f"DLQ event {event_id} misses kafka metadata: {message!r}")
                ok(f"DLQ contains {event_id} with {error_code}")
                return message
        time.sleep(1)
    fail(f"DLQ message for {event_id} not found; last messages={last_messages!r}")


def assert_cassandra_nodes(count, label, *, timeout=60):
    last_output = None

    def matches():
        nonlocal last_output
        proc = run_cmd(["docker", "exec", "soa-hw6-cassandra-1", "nodetool", "status"], echo=False)
        last_output = proc.stdout
        return len(re.findall(r"^UN\s+", proc.stdout, flags=re.MULTILINE)) == count

    try:
        wait_until(label, matches, timeout=timeout, interval=2)
    except E2EError as exc:
        raise E2EError(f"{exc}; nodetool status:\n{last_output}") from exc


def preflight():
    section("Preflight")
    wait_until("consumer health is ok", lambda: http_json(f"{CONSUMER_URL}/health").get("status") == "ok", timeout=60)
    wait_until("Schema Registry subject is available", lambda: 1 in http_json(f"{SR_URL}/subjects/warehouse-events-value/versions"), timeout=60)
    assert_cassandra_nodes(3, "Cassandra has 3 UN nodes", timeout=90)


def scenario_1(run_id):
    section("Scenario 1: basic warehouse cycle")
    sku = f"SKU-001-{run_id}"
    order_id = f"ORD-DEMO-2-{run_id}"

    produce(event_type="PRODUCT_RECEIVED", product_id=sku, zone_id="ZONE-A", quantity=100)
    assert_row(
        ["product_id", "zone_id", "available_quantity", "reserved_quantity"],
        "warehouse.inventory_by_product_zone",
        f"product_id={cql_quote(sku)} AND zone_id='ZONE-A'",
        {"available_quantity": 100, "reserved_quantity": 0},
        "PRODUCT_RECEIVED writes available=100 reserved=0",
    )
    assert_row(
        ["product_id", "total_available"],
        "warehouse.inventory_totals",
        f"product_id={cql_quote(sku)}",
        {"total_available": 100},
        "inventory_totals total_available=100",
    )

    produce(event_type="PRODUCT_RESERVED", product_id=sku, zone_id="ZONE-A", quantity=30, order_id=f"ORD-RES-{run_id}")
    assert_row(
        ["product_id", "zone_id", "available_quantity", "reserved_quantity"],
        "warehouse.inventory_by_product_zone",
        f"product_id={cql_quote(sku)} AND zone_id='ZONE-A'",
        {"available_quantity": 70, "reserved_quantity": 30},
        "PRODUCT_RESERVED moves 30 from available to reserved",
    )

    produce(event_type="PRODUCT_MOVED", product_id=sku, from_zone_id="ZONE-A", to_zone_id="ZONE-B", quantity=20)
    assert_rows(
        ["product_id", "zone_id", "available_quantity", "reserved_quantity"],
        "warehouse.inventory_by_product_zone",
        f"product_id={cql_quote(sku)}",
        [
            {"product_id": sku, "zone_id": "ZONE-A", "available_quantity": 50, "reserved_quantity": 30},
            {"product_id": sku, "zone_id": "ZONE-B", "available_quantity": 20, "reserved_quantity": 0},
        ],
        "PRODUCT_MOVED leaves reserved stock intact",
    )

    produce(event_type="PRODUCT_SHIPPED", product_id=sku, zone_id="ZONE-A", quantity=10)
    assert_row(
        ["product_id", "zone_id", "available_quantity", "reserved_quantity"],
        "warehouse.inventory_by_product_zone",
        f"product_id={cql_quote(sku)} AND zone_id='ZONE-A'",
        {"available_quantity": 40, "reserved_quantity": 30},
        "PRODUCT_SHIPPED decrements available to 40 and keeps reserved at 30",
    )

    produce(event_type="ORDER_CREATED", product_id=sku, zone_id="ZONE-A", quantity=15, order_id=order_id)
    assert_row(
        ["order_id", "status"],
        "warehouse.orders",
        f"order_id={cql_quote(order_id)}",
        {"status": "CREATED"},
        "ORDER_CREATED creates order",
    )
    assert_row(
        ["product_id", "zone_id", "available_quantity", "reserved_quantity"],
        "warehouse.inventory_by_product_zone",
        f"product_id={cql_quote(sku)} AND zone_id='ZONE-A'",
        {"available_quantity": 25, "reserved_quantity": 45},
        "ORDER_CREATED moves 15 from available to reserved",
    )

    produce(event_type="ORDER_COMPLETED", order_id=order_id)
    assert_row(
        ["order_id", "status"],
        "warehouse.orders",
        f"order_id={cql_quote(order_id)}",
        {"status": "COMPLETED"},
        "ORDER_COMPLETED completes order",
    )
    assert_row(
        ["product_id", "zone_id", "available_quantity", "reserved_quantity"],
        "warehouse.inventory_by_product_zone",
        f"product_id={cql_quote(sku)} AND zone_id='ZONE-A'",
        {"available_quantity": 25, "reserved_quantity": 30},
        "ORDER_COMPLETED releases reserved quantity without changing available",
    )
    assert_row(
        ["product_id", "total_available", "total_reserved"],
        "warehouse.inventory_totals",
        f"product_id={cql_quote(sku)}",
        {"total_available": 45, "total_reserved": 30},
        "inventory_totals matches final per-zone state",
    )


def scenario_2(run_id):
    section("Scenario 2: idempotency")
    sku = f"SKU-002-{run_id}"
    event_id = f"evt-idempotent-{run_id}"

    produce(event_id=event_id, event_type="PRODUCT_RECEIVED", product_id=sku, zone_id="ZONE-A", quantity=50)
    assert_row(
        ["product_id", "zone_id", "available_quantity"],
        "warehouse.inventory_by_product_zone",
        f"product_id={cql_quote(sku)} AND zone_id='ZONE-A'",
        {"available_quantity": 50},
        "first event writes available=50",
    )

    produce(event_id=event_id, event_type="PRODUCT_RECEIVED", product_id=sku, zone_id="ZONE-A", quantity=50)
    assert_row(
        ["product_id", "zone_id", "available_quantity"],
        "warehouse.inventory_by_product_zone",
        f"product_id={cql_quote(sku)} AND zone_id='ZONE-A'",
        {"available_quantity": 50},
        "duplicate event is ignored",
    )


def scenario_3(run_id):
    section("Scenario 3: table consistency")
    sku = f"SKU-003-{run_id}"

    produce(event_type="PRODUCT_RECEIVED", product_id=sku, zone_id="ZONE-A", quantity=100)
    assert_row(
        ["product_id", "zone_id", "available_quantity"],
        "warehouse.inventory_by_product_zone",
        f"product_id={cql_quote(sku)} AND zone_id='ZONE-A'",
        {"available_quantity": 100},
        "inventory_by_product_zone is consistent",
    )
    assert_row(
        ["zone_id", "product_id", "available_quantity"],
        "warehouse.inventory_by_zone",
        f"zone_id='ZONE-A' AND product_id={cql_quote(sku)}",
        {"available_quantity": 100},
        "inventory_by_zone is consistent",
    )
    assert_row(
        ["product_id", "total_available"],
        "warehouse.inventory_totals",
        f"product_id={cql_quote(sku)}",
        {"total_available": 100},
        "inventory_totals is consistent",
    )


def scenario_4(run_id):
    section("Scenario 4: out-of-order events")
    sku = f"SKU-004-{run_id}"
    now_ms = int(time.time() * 1000)
    t1 = now_ms - 300_000
    t2 = now_ms
    t3 = now_ms - 180_000

    produce(event_id=f"evt-oor-1-{run_id}", timestamp=t1, event_type="PRODUCT_RECEIVED", product_id=sku, zone_id="ZONE-A", quantity=100)
    assert_row(
        ["product_id", "zone_id", "available_quantity"],
        "warehouse.inventory_by_product_zone",
        f"product_id={cql_quote(sku)} AND zone_id='ZONE-A'",
        {"available_quantity": 100},
        "older initial receive writes available=100",
    )

    produce(event_id=f"evt-oor-2-{run_id}", timestamp=t2, event_type="PRODUCT_SHIPPED", product_id=sku, zone_id="ZONE-A", quantity=20)
    assert_row(
        ["product_id", "zone_id", "available_quantity"],
        "warehouse.inventory_by_product_zone",
        f"product_id={cql_quote(sku)} AND zone_id='ZONE-A'",
        {"available_quantity": 80},
        "newer shipped event writes available=80",
    )

    produce(event_id=f"evt-oor-3-{run_id}", timestamp=t3, event_type="PRODUCT_RECEIVED", product_id=sku, zone_id="ZONE-A", quantity=50)
    assert_row(
        ["product_id", "zone_id", "available_quantity"],
        "warehouse.inventory_by_product_zone",
        f"product_id={cql_quote(sku)} AND zone_id='ZONE-A'",
        {"available_quantity": 80},
        "older event is ignored",
    )


def scenario_5(run_id):
    section("Scenario 5: Dead Letter Queue")
    bad_sku = f"SKU-005-BAD-{run_id}"
    good_sku = f"SKU-005-{run_id}"
    bad_event_id = f"evt-dlq-{run_id}"

    produce(event_id=bad_event_id, event_type="PRODUCT_SHIPPED", product_id=bad_sku, zone_id="ZONE-A", quantity=-5)
    assert_dlq_event(bad_event_id, "VALIDATION_ERROR")

    produce(event_type="PRODUCT_RECEIVED", product_id=good_sku, zone_id="ZONE-A", quantity=10)
    assert_row(
        ["product_id", "zone_id", "available_quantity"],
        "warehouse.inventory_by_product_zone",
        f"product_id={cql_quote(good_sku)} AND zone_id='ZONE-A'",
        {"available_quantity": 10},
        "consumer continues after DLQ event",
    )


def scenario_6(run_id):
    section("Scenario 6: Cassandra 3-node cluster and failover")
    sku = f"SKU-006-{run_id}"
    assert_cassandra_nodes(3, "Cassandra starts with 3 UN nodes")

    produce(event_type="PRODUCT_RECEIVED", product_id=sku, zone_id="ZONE-A", quantity=200)
    assert_row(
        ["product_id", "zone_id", "available_quantity"],
        "warehouse.inventory_by_product_zone",
        f"product_id={cql_quote(sku)} AND zone_id='ZONE-A'",
        {"available_quantity": 200},
        "write succeeds before failover",
    )

    print("Stopping cassandra-2...", flush=True)
    run_cmd(["docker", "stop", "soa-hw6-cassandra-2"], timeout=60)
    try:
        produce(event_type="PRODUCT_SHIPPED", product_id=sku, zone_id="ZONE-A", quantity=50)
        assert_row(
            ["product_id", "zone_id", "available_quantity"],
            "warehouse.inventory_by_product_zone",
            f"product_id={cql_quote(sku)} AND zone_id='ZONE-A'",
            {"available_quantity": 150},
            "system works with 2/3 Cassandra nodes",
            timeout=45,
        )
    finally:
        print("Starting cassandra-2 back...", flush=True)
        run_cmd(["docker", "start", "soa-hw6-cassandra-2"], check=False, timeout=60)

    assert_cassandra_nodes(3, "Cassandra returns to 3 UN nodes", timeout=120)


def scenario_7(run_id):
    section("Scenario 7: monitoring and consumer lag")
    health = http_json(f"{CONSUMER_URL}/health")
    if health.get("status") != "ok":
        fail(f"health is not ok: {health!r}")
    ok("health endpoint returns ok")

    wait_until(
        "Kafka exporter exposes consumer group lag",
        lambda: metric_values(read_kafka_exporter_metrics(), "kafka_consumergroup_lag"),
        timeout=45,
        interval=2,
    )

    wait_until(
        "Prometheus scrapes Kafka exporter",
        lambda: any(float(item["value"][1]) == 1 for item in prometheus_query('up{job="kafka-exporter"}')),
        timeout=45,
        interval=2,
    )
    assert_prometheus_alert_rule()

    metrics = read_metrics()
    processed_values = metric_values(metrics, "warehouse_events_processed_total")
    if not processed_values or sum(processed_values) < 1:
        fail("warehouse_events_processed_total did not increase")
    ok("processed events metric increased")

    lag_values = metric_values(metrics, "warehouse_consumer_lag")
    if not lag_values:
        fail("warehouse_consumer_lag metric is missing")
    if any(value != 0 for value in lag_values):
        fail(f"consumer lag is not zero: {lag_values!r}")
    ok("consumer lag metrics are present and equal to 0")

    dlq_values = metric_values(metrics, "warehouse_dlq_events_total")
    if not dlq_values or sum(dlq_values) < 1:
        fail("warehouse_dlq_events_total did not increase")
    ok("DLQ metric increased")

    duration_values = metric_values(metrics, "warehouse_event_processing_duration_seconds_count")
    if not duration_values or sum(duration_values) < 1:
        fail("event processing duration metrics are missing")
    ok("event processing metrics are present")

    backlog_sku = f"SKU-007-LAG-{run_id}"
    print("Stopping warehouse-consumer to create Kafka backlog...", flush=True)
    run_cmd(["docker", "stop", "soa-hw6-warehouse-consumer"], timeout=60)
    try:
        produce(
            event_id=f"evt-lag-{run_id}",
            event_type="PRODUCT_RECEIVED",
            product_id=backlog_sku,
            zone_id="ZONE-A",
            quantity=1,
            count=120,
        )
        wait_until(
            "Kafka consumer group lag is above the alert threshold",
            lambda: consumer_group_lag() > 100,
            timeout=30,
            interval=2,
        )
        wait_until(
            "Kafka exporter reports lag above the alert threshold",
            lambda: sum(metric_values(read_kafka_exporter_metrics(), "kafka_consumergroup_lag")) > 100,
            timeout=75,
            interval=5,
        )
        wait_until(
            "Prometheus lag alert is firing",
            lambda: alert_state("WarehouseConsumerLagHigh") == "firing",
            timeout=100,
            interval=5,
        )
    finally:
        print("Starting warehouse-consumer back...", flush=True)
        run_cmd(["docker", "start", "soa-hw6-warehouse-consumer"], check=False, timeout=60)

    wait_until("consumer health is ok after restart", lambda: http_json(f"{CONSUMER_URL}/health").get("status") == "ok", timeout=90)
    wait_until("Kafka consumer group lag returns to zero", lambda: consumer_group_lag() == 0, timeout=90, interval=2)
    wait_until(
        "Kafka exporter lag returns to zero",
        lambda: (values := metric_values(read_kafka_exporter_metrics(), "kafka_consumergroup_lag")) and sum(values) == 0,
        timeout=90,
        interval=5,
    )
    wait_until(
        "consumer lag metrics return to zero",
        lambda: (values := metric_values(read_metrics(), "warehouse_consumer_lag")) and all(value == 0 for value in values),
        timeout=90,
        interval=2,
    )
    assert_row(
        ["product_id", "zone_id", "available_quantity"],
        "warehouse.inventory_by_product_zone",
        f"product_id={cql_quote(backlog_sku)} AND zone_id='ZONE-A'",
        {"available_quantity": 120},
        "consumer processes backlog after restart",
        timeout=45,
    )


def scenario_8(run_id):
    section("Scenario 8: schema evolution (V1 -> V2)")
    versions_before = http_json(f"{SR_URL}/subjects/warehouse-events-value/versions")
    if 1 not in versions_before:
        fail(f"Schema Registry does not contain V1: {versions_before!r}")
    ok(f"Schema Registry has V1; current versions={versions_before}")

    sku_without_supplier = f"SKU-008-{run_id}"
    sku_with_supplier = f"SKU-008v2-{run_id}"

    produce(schema_version=1, event_type="PRODUCT_RECEIVED", product_id=sku_without_supplier, zone_id="ZONE-A", quantity=50)
    assert_row(
        ["product_id", "zone_id", "available_quantity", "supplier_id"],
        "warehouse.inventory_by_product_zone",
        f"product_id={cql_quote(sku_without_supplier)} AND zone_id='ZONE-A'",
        {"available_quantity": 50, "supplier_id": None},
        "V1 event without supplier_id stores supplier_id=null",
    )

    produce(schema_version=2, event_type="PRODUCT_RECEIVED", product_id=sku_with_supplier, zone_id="ZONE-A", quantity=50, supplier_id="SUP-001")
    assert_row(
        ["product_id", "zone_id", "available_quantity", "supplier_id"],
        "warehouse.inventory_by_product_zone",
        f"product_id={cql_quote(sku_with_supplier)} AND zone_id='ZONE-A'",
        {"available_quantity": 50, "supplier_id": "SUP-001"},
        "V2 event stores supplier_id=SUP-001",
    )

    versions_after = http_json(f"{SR_URL}/subjects/warehouse-events-value/versions")
    if 1 not in versions_after or 2 not in versions_after:
        fail(f"Schema Registry must contain V1 and V2, got {versions_after!r}")
    ok(f"Schema Registry has V1 and V2; current versions={versions_after}")

    config = http_json(f"{SR_URL}/config")
    if config.get("compatibilityLevel") != "BACKWARD":
        fail(f"Schema Registry compatibility is not BACKWARD: {config!r}")
    ok("Schema Registry compatibility is BACKWARD")


def main():
    parser = argparse.ArgumentParser(description="Smart Warehouse E2E assertions")
    parser.add_argument("--run-id", default=None, help="unique suffix for products/events")
    args = parser.parse_args()

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    print(f"E2E run id: {run_id}")

    preflight()
    scenario_1(run_id)
    scenario_2(run_id)
    scenario_3(run_id)
    scenario_4(run_id)
    scenario_5(run_id)
    scenario_6(run_id)
    scenario_7(run_id)
    scenario_8(run_id)

    section("All scenarios passed")


if __name__ == "__main__":
    try:
        main()
    except (E2EError, AssertionError, subprocess.SubprocessError, urllib.error.URLError) as exc:
        print(f"\nE2E FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
