#!/usr/bin/env python3
"""Register V1 warehouse event schema in Schema Registry.

Runs as a docker-compose init container before warehouse-consumer starts.
Exits 0 if schema is already registered.
"""
import json
import sys
import urllib.error
import urllib.request

SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
SUBJECT = "warehouse-events-value"
SCHEMA_FILE = "/migrations/events_v1.proto"

with open(SCHEMA_FILE) as f:
    schema_text = f.read()

payload = json.dumps({"schemaType": "PROTOBUF", "schema": schema_text}).encode()

req = urllib.request.Request(
    f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions",
    data=payload,
    headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    method="POST",
)

try:
    resp = urllib.request.urlopen(req)
    result = json.loads(resp.read())
    print(f"V1 schema registered: subject={SUBJECT} id={result['id']}", flush=True)
except urllib.error.HTTPError as e:
    body = e.read().decode()
    if e.code == 409:
        print(f"V1 schema already registered (idempotent): {body}", flush=True)
        sys.exit(0)
    print(f"Error {e.code}: {body}", file=sys.stderr, flush=True)
    sys.exit(1)