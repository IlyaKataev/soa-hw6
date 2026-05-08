package cassandra

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

const Keyspace = "warehouse"

type Store struct {
	session *gocql.Session
}

type Inventory struct {
	Available  int64
	Reserved   int64
	LastEvent  time.Time
	SupplierID *string
}

type Totals struct {
	Available int64
	Reserved  int64
	LastEvent time.Time
}

type Order struct {
	OrderID   string
	Status    string
	ItemsJSON string
	LastEvent time.Time
}

func NewSession(hosts []string, dc string) (*gocql.Session, error) {
	if len(hosts) == 0 {
		hosts = []string{"127.0.0.1:9042"}
	}
	if dc == "" {
		dc = "datacenter1"
	}

	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = Keyspace
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}
	cluster.PoolConfig.HostSelectionPolicy = gocql.DCAwareRoundRobinPolicy(dc)

	return cluster.CreateSession()
}

func NewStore(session *gocql.Session) *Store {
	return &Store{session: session}
}

func (s *Store) Close() {
	if s.session != nil {
		s.session.Close()
	}
}

func (s *Store) Session() *gocql.Session {
	return s.session
}

func HostsFromEnv() []string {
	raw := strings.TrimSpace(os.Getenv("CASSANDRA_HOSTS"))
	if raw == "" {
		return []string{"cassandra-1:9042", "cassandra-2:9042", "cassandra-3:9042"}
	}
	parts := strings.Split(raw, ",")
	hosts := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			hosts = append(hosts, part)
		}
	}
	return hosts
}

func (s *Store) GetInventory(ctx context.Context, productID, zoneID string) (Inventory, error) {
	var inv Inventory
	var supplier *string
	err := s.session.Query(
		`SELECT available_quantity, reserved_quantity, last_event_timestamp, supplier_id
		 FROM inventory_by_product_zone WHERE product_id = ? AND zone_id = ?`,
		productID, zoneID,
	).WithContext(ctx).Consistency(gocql.Quorum).Scan(&inv.Available, &inv.Reserved, &inv.LastEvent, &supplier)
	if errors.Is(err, gocql.ErrNotFound) {
		return Inventory{}, nil
	}
	if err != nil {
		return Inventory{}, err
	}
	inv.SupplierID = supplier
	return inv, nil
}

func (s *Store) GetTotals(ctx context.Context, productID string) (Totals, error) {
	var totals Totals
	err := s.session.Query(
		`SELECT total_available, total_reserved, last_event_timestamp
		 FROM inventory_totals WHERE product_id = ?`,
		productID,
	).WithContext(ctx).Consistency(gocql.Quorum).Scan(&totals.Available, &totals.Reserved, &totals.LastEvent)
	if errors.Is(err, gocql.ErrNotFound) {
		return Totals{}, nil
	}
	return totals, err
}

func (s *Store) GetLastEventTimestamp(ctx context.Context, productID, zoneID string) (time.Time, error) {
	inv, err := s.GetInventory(ctx, productID, zoneID)
	if err != nil {
		return time.Time{}, err
	}
	return inv.LastEvent, nil
}

func (s *Store) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	var processedAt time.Time
	err := s.session.Query(
		`SELECT processed_at FROM processed_events WHERE event_id = ?`,
		eventID,
	).WithContext(ctx).Consistency(gocql.Quorum).Scan(&processedAt)
	if errors.Is(err, gocql.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (s *Store) GetOrder(ctx context.Context, orderID string) (Order, error) {
	var order Order
	err := s.session.Query(
		`SELECT order_id, status, items, last_event_timestamp FROM orders WHERE order_id = ?`,
		orderID,
	).WithContext(ctx).Consistency(gocql.Quorum).Scan(&order.OrderID, &order.Status, &order.ItemsJSON, &order.LastEvent)
	return order, err
}

func (s *Store) Health(ctx context.Context) error {
	var eventID string
	err := s.session.Query(
		`SELECT event_id FROM processed_events LIMIT 1`,
	).WithContext(ctx).Consistency(gocql.One).Scan(&eventID)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil
	}
	return err
}
