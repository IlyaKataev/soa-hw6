package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"google.golang.org/protobuf/encoding/protojson"

	pb "warehouse/gen/warehouse/v1"
	"warehouse/internal/cassandra"
)

type Handler struct {
	store *cassandra.Store
	log   *slog.Logger
}

type orderItem struct {
	ProductID string `json:"product_id"`
	ZoneID    string `json:"zone_id"`
	Quantity  int64  `json:"quantity"`
}

func New(store *cassandra.Store, log *slog.Logger) *Handler {
	return &Handler{store: store, log: log}
}

func (h *Handler) Handle(ctx context.Context, event *pb.WarehouseEvent) error {
	if event == nil {
		return validationError("nil event")
	}
	if event.EventId == "" {
		return validationError("event_id is required")
	}
	if event.Timestamp <= 0 {
		return validationError("timestamp is required")
	}

	processed, err := h.store.IsProcessed(ctx, event.EventId)
	if err != nil {
		return fmt.Errorf("check processed event: %w", err)
	}
	if processed {
		h.log.Info("duplicate skipped", "event_id", event.EventId, "event_type", event.EventType)
		return nil
	}

	switch payload := event.Payload.(type) {
	case *pb.WarehouseEvent_ProductReceived:
		return h.handleProductReceived(ctx, event, payload.ProductReceived)
	case *pb.WarehouseEvent_ProductShipped:
		return h.handleProductShipped(ctx, event, payload.ProductShipped)
	case *pb.WarehouseEvent_ProductMoved:
		return h.handleProductMoved(ctx, event, payload.ProductMoved)
	case *pb.WarehouseEvent_ProductReserved:
		return h.handleProductReserved(ctx, event, payload.ProductReserved)
	case *pb.WarehouseEvent_ProductReleased:
		return h.handleProductReleased(ctx, event, payload.ProductReleased)
	case *pb.WarehouseEvent_InventoryCounted:
		return h.handleInventoryCounted(ctx, event, payload.InventoryCounted)
	case *pb.WarehouseEvent_OrderCreated:
		return h.handleOrderCreated(ctx, event, payload.OrderCreated)
	case *pb.WarehouseEvent_OrderCompleted:
		return h.handleOrderCompleted(ctx, event, payload.OrderCompleted)
	default:
		return validationError("unknown payload for event_type=%q", event.EventType)
	}
}

func (h *Handler) handleProductReceived(ctx context.Context, event *pb.WarehouseEvent, payload *pb.ProductReceived) error {
	if err := validateProductZoneQty(payload.GetProductId(), payload.GetZoneId(), int64(payload.GetQuantity())); err != nil {
		return err
	}
	eventTime := eventTime(event)
	if skip, err := h.isOldZoneEvent(ctx, eventTime, payload.GetProductId(), payload.GetZoneId()); err != nil || skip {
		return err
	}

	inv, totals, err := h.readInventoryAndTotals(ctx, payload.GetProductId(), payload.GetZoneId())
	if err != nil {
		return err
	}
	qty := int64(payload.GetQuantity())
	inv.Available += qty
	totals.Available += qty
	inv.LastEvent = eventTime
	totals.LastEvent = eventTime
	if payload.SupplierId != nil {
		inv.SupplierID = payload.SupplierId
	}

	batch := h.newBatch(event)
	h.putInventory(batch, payload.GetProductId(), payload.GetZoneId(), inv)
	h.putZoneInventory(batch, payload.GetZoneId(), payload.GetProductId(), inv, eventTime)
	h.putTotals(batch, payload.GetProductId(), totals)
	h.putHistory(batch, payload.GetProductId(), event)
	return h.execBatch(ctx, batch)
}

func (h *Handler) handleProductShipped(ctx context.Context, event *pb.WarehouseEvent, payload *pb.ProductShipped) error {
	if err := validateProductZoneQty(payload.GetProductId(), payload.GetZoneId(), int64(payload.GetQuantity())); err != nil {
		return err
	}
	eventTime := eventTime(event)
	if skip, err := h.isOldZoneEvent(ctx, eventTime, payload.GetProductId(), payload.GetZoneId()); err != nil || skip {
		return err
	}
	inv, totals, err := h.readInventoryAndTotals(ctx, payload.GetProductId(), payload.GetZoneId())
	if err != nil {
		return err
	}
	qty := int64(payload.GetQuantity())
	inv.Available -= qty
	totals.Available -= qty
	if inv.Available < 0 || totals.Available < 0 {
		return validationError("shipping %d would make available stock negative", qty)
	}
	inv.LastEvent = eventTime
	totals.LastEvent = eventTime

	batch := h.newBatch(event)
	h.putInventory(batch, payload.GetProductId(), payload.GetZoneId(), inv)
	h.putZoneInventory(batch, payload.GetZoneId(), payload.GetProductId(), inv, eventTime)
	h.putTotals(batch, payload.GetProductId(), totals)
	h.putHistory(batch, payload.GetProductId(), event)
	return h.execBatch(ctx, batch)
}

func (h *Handler) handleProductMoved(ctx context.Context, event *pb.WarehouseEvent, payload *pb.ProductMoved) error {
	if payload.GetProductId() == "" || payload.GetFromZoneId() == "" || payload.GetToZoneId() == "" {
		return validationError("product_id, from_zone_id and to_zone_id are required")
	}
	if payload.GetFromZoneId() == payload.GetToZoneId() {
		return validationError("from_zone_id and to_zone_id must differ")
	}
	if payload.GetQuantity() <= 0 {
		return validationError("invalid quantity %d", payload.GetQuantity())
	}
	eventTime := eventTime(event)
	if skip, err := h.isOldZoneEvent(ctx, eventTime, payload.GetProductId(), payload.GetFromZoneId()); err != nil || skip {
		return err
	}
	if skip, err := h.isOldZoneEvent(ctx, eventTime, payload.GetProductId(), payload.GetToZoneId()); err != nil || skip {
		return err
	}

	fromInv, err := h.store.GetInventory(ctx, payload.GetProductId(), payload.GetFromZoneId())
	if err != nil {
		return err
	}
	toInv, err := h.store.GetInventory(ctx, payload.GetProductId(), payload.GetToZoneId())
	if err != nil {
		return err
	}
	qty := int64(payload.GetQuantity())
	fromInv.Available -= qty
	toInv.Available += qty
	if fromInv.Available < 0 {
		return validationError("moving %d would make source zone stock negative", qty)
	}
	fromInv.LastEvent = eventTime
	toInv.LastEvent = eventTime

	batch := h.newBatch(event)
	h.putInventory(batch, payload.GetProductId(), payload.GetFromZoneId(), fromInv)
	h.putInventory(batch, payload.GetProductId(), payload.GetToZoneId(), toInv)
	h.putZoneInventory(batch, payload.GetFromZoneId(), payload.GetProductId(), fromInv, eventTime)
	h.putZoneInventory(batch, payload.GetToZoneId(), payload.GetProductId(), toInv, eventTime)
	h.putHistory(batch, payload.GetProductId(), event)
	return h.execBatch(ctx, batch)
}

func (h *Handler) handleProductReserved(ctx context.Context, event *pb.WarehouseEvent, payload *pb.ProductReserved) error {
	if err := validateProductZoneQty(payload.GetProductId(), payload.GetZoneId(), int64(payload.GetQuantity())); err != nil {
		return err
	}
	if payload.GetOrderId() == "" {
		return validationError("order_id is required")
	}
	eventTime := eventTime(event)
	if skip, err := h.isOldZoneEvent(ctx, eventTime, payload.GetProductId(), payload.GetZoneId()); err != nil || skip {
		return err
	}
	inv, totals, err := h.readInventoryAndTotals(ctx, payload.GetProductId(), payload.GetZoneId())
	if err != nil {
		return err
	}
	qty := int64(payload.GetQuantity())
	inv.Available -= qty
	inv.Reserved += qty
	totals.Available -= qty
	totals.Reserved += qty
	if inv.Available < 0 || totals.Available < 0 {
		return validationError("reserving %d would make available stock negative", qty)
	}
	inv.LastEvent = eventTime
	totals.LastEvent = eventTime

	batch := h.newBatch(event)
	h.putInventory(batch, payload.GetProductId(), payload.GetZoneId(), inv)
	h.putZoneInventory(batch, payload.GetZoneId(), payload.GetProductId(), inv, eventTime)
	h.putTotals(batch, payload.GetProductId(), totals)
	h.putHistory(batch, payload.GetProductId(), event)
	return h.execBatch(ctx, batch)
}

func (h *Handler) handleProductReleased(ctx context.Context, event *pb.WarehouseEvent, payload *pb.ProductReleased) error {
	if err := validateProductZoneQty(payload.GetProductId(), payload.GetZoneId(), int64(payload.GetQuantity())); err != nil {
		return err
	}
	if payload.GetOrderId() == "" {
		return validationError("order_id is required")
	}
	eventTime := eventTime(event)
	if skip, err := h.isOldZoneEvent(ctx, eventTime, payload.GetProductId(), payload.GetZoneId()); err != nil || skip {
		return err
	}
	inv, totals, err := h.readInventoryAndTotals(ctx, payload.GetProductId(), payload.GetZoneId())
	if err != nil {
		return err
	}
	qty := int64(payload.GetQuantity())
	inv.Available += qty
	inv.Reserved -= qty
	totals.Available += qty
	totals.Reserved -= qty
	if inv.Reserved < 0 || totals.Reserved < 0 {
		return validationError("releasing %d would make reserved stock negative", qty)
	}
	inv.LastEvent = eventTime
	totals.LastEvent = eventTime

	batch := h.newBatch(event)
	h.putInventory(batch, payload.GetProductId(), payload.GetZoneId(), inv)
	h.putZoneInventory(batch, payload.GetZoneId(), payload.GetProductId(), inv, eventTime)
	h.putTotals(batch, payload.GetProductId(), totals)
	h.putHistory(batch, payload.GetProductId(), event)
	return h.execBatch(ctx, batch)
}

func (h *Handler) handleInventoryCounted(ctx context.Context, event *pb.WarehouseEvent, payload *pb.InventoryCounted) error {
	if payload.GetProductId() == "" || payload.GetZoneId() == "" {
		return validationError("product_id and zone_id are required")
	}
	if payload.GetCountedQuantity() < 0 {
		return validationError("invalid counted_quantity %d", payload.GetCountedQuantity())
	}
	eventTime := eventTime(event)
	if skip, err := h.isOldZoneEvent(ctx, eventTime, payload.GetProductId(), payload.GetZoneId()); err != nil || skip {
		return err
	}
	inv, totals, err := h.readInventoryAndTotals(ctx, payload.GetProductId(), payload.GetZoneId())
	if err != nil {
		return err
	}
	oldAvailable := inv.Available
	inv.Available = int64(payload.GetCountedQuantity())
	totals.Available += inv.Available - oldAvailable
	inv.LastEvent = eventTime
	totals.LastEvent = eventTime

	batch := h.newBatch(event)
	h.putInventory(batch, payload.GetProductId(), payload.GetZoneId(), inv)
	h.putZoneInventory(batch, payload.GetZoneId(), payload.GetProductId(), inv, eventTime)
	h.putTotals(batch, payload.GetProductId(), totals)
	h.putHistory(batch, payload.GetProductId(), event)
	return h.execBatch(ctx, batch)
}

func (h *Handler) handleOrderCreated(ctx context.Context, event *pb.WarehouseEvent, payload *pb.OrderCreated) error {
	if payload.GetOrderId() == "" {
		return validationError("order_id is required")
	}
	items, err := normalizeOrderItems(payload.GetItems())
	if err != nil {
		return err
	}
	eventTime := eventTime(event)
	if skip, err := h.isOldOrderEvent(ctx, eventTime, payload.GetOrderId()); err != nil || skip {
		return err
	}
	itemsJSON, err := json.Marshal(items)
	if err != nil {
		return err
	}

	batch := h.newBatch(event)
	batch.Query(
		`INSERT INTO orders (order_id, status, created_at, items, last_event_timestamp)
		 VALUES (?, ?, ?, ?, ?)`,
		payload.GetOrderId(), "CREATED", eventTime, string(itemsJSON), eventTime,
	)
	for _, item := range items {
		inv, totals, err := h.readInventoryAndTotals(ctx, item.ProductID, item.ZoneID)
		if err != nil {
			return err
		}
		inv.Available -= item.Quantity
		inv.Reserved += item.Quantity
		totals.Available -= item.Quantity
		totals.Reserved += item.Quantity
		if inv.Available < 0 || totals.Available < 0 {
			return validationError("order %s would make available stock negative for product=%s zone=%s", payload.GetOrderId(), item.ProductID, item.ZoneID)
		}
		inv.LastEvent = eventTime
		totals.LastEvent = eventTime
		h.putInventory(batch, item.ProductID, item.ZoneID, inv)
		h.putZoneInventory(batch, item.ZoneID, item.ProductID, inv, eventTime)
		h.putTotals(batch, item.ProductID, totals)
		h.putHistory(batch, item.ProductID, event)
	}
	return h.execBatch(ctx, batch)
}

func (h *Handler) handleOrderCompleted(ctx context.Context, event *pb.WarehouseEvent, payload *pb.OrderCompleted) error {
	if payload.GetOrderId() == "" {
		return validationError("order_id is required")
	}
	eventTime := eventTime(event)
	if skip, err := h.isOldOrderEvent(ctx, eventTime, payload.GetOrderId()); err != nil || skip {
		return err
	}
	order, err := h.store.GetOrder(ctx, payload.GetOrderId())
	if errors.Is(err, gocql.ErrNotFound) {
		return validationError("order %s not found", payload.GetOrderId())
	}
	if err != nil {
		return err
	}
	if order.Status == "COMPLETED" {
		h.log.Info("already completed order skipped", "order_id", payload.GetOrderId(), "event_id", event.EventId)
		return nil
	}
	var items []orderItem
	if err := json.Unmarshal([]byte(order.ItemsJSON), &items); err != nil {
		return fmt.Errorf("decode order items: %w", err)
	}

	batch := h.newBatch(event)
	batch.Query(
		`UPDATE orders SET status = ?, completed_at = ?, last_event_timestamp = ?
		 WHERE order_id = ?`,
		"COMPLETED", eventTime, eventTime, payload.GetOrderId(),
	)
	for _, item := range items {
		inv, totals, err := h.readInventoryAndTotals(ctx, item.ProductID, item.ZoneID)
		if err != nil {
			return err
		}
		inv.Reserved -= item.Quantity
		totals.Reserved -= item.Quantity
		if inv.Reserved < 0 || totals.Reserved < 0 {
			return validationError("completing order %s would make reserved stock negative for product=%s zone=%s", payload.GetOrderId(), item.ProductID, item.ZoneID)
		}
		inv.LastEvent = eventTime
		totals.LastEvent = eventTime
		h.putInventory(batch, item.ProductID, item.ZoneID, inv)
		h.putZoneInventory(batch, item.ZoneID, item.ProductID, inv, eventTime)
		h.putTotals(batch, item.ProductID, totals)
		h.putHistory(batch, item.ProductID, event)
	}
	return h.execBatch(ctx, batch)
}

func (h *Handler) readInventoryAndTotals(ctx context.Context, productID, zoneID string) (cassandra.Inventory, cassandra.Totals, error) {
	inv, err := h.store.GetInventory(ctx, productID, zoneID)
	if err != nil {
		return cassandra.Inventory{}, cassandra.Totals{}, err
	}
	totals, err := h.store.GetTotals(ctx, productID)
	if err != nil {
		return cassandra.Inventory{}, cassandra.Totals{}, err
	}
	return inv, totals, nil
}

func (h *Handler) newBatch(event *pb.WarehouseEvent) *gocql.Batch {
	batch := h.store.Session().NewBatch(gocql.LoggedBatch).Consistency(gocql.Quorum)
	batch.Query(
		`INSERT INTO processed_events (event_id, processed_at, event_type) VALUES (?, ?, ?)`,
		event.EventId, time.Now().UTC(), event.EventType,
	)
	return batch
}

func (h *Handler) execBatch(ctx context.Context, batch *gocql.Batch) error {
	return h.store.Session().ExecuteBatch(batch.WithContext(ctx))
}

func (h *Handler) putInventory(batch *gocql.Batch, productID, zoneID string, inv cassandra.Inventory) {
	batch.Query(
		`UPDATE inventory_by_product_zone
		 SET available_quantity = ?, reserved_quantity = ?, last_event_timestamp = ?, supplier_id = ?
		 WHERE product_id = ? AND zone_id = ?`,
		inv.Available, inv.Reserved, inv.LastEvent, inv.SupplierID, productID, zoneID,
	)
}

func (h *Handler) putZoneInventory(batch *gocql.Batch, zoneID, productID string, inv cassandra.Inventory, eventTime time.Time) {
	batch.Query(
		`UPDATE inventory_by_zone
		 SET available_quantity = ?, reserved_quantity = ?, last_event_timestamp = ?
		 WHERE zone_id = ? AND product_id = ?`,
		inv.Available, inv.Reserved, eventTime, zoneID, productID,
	)
}

func (h *Handler) putTotals(batch *gocql.Batch, productID string, totals cassandra.Totals) {
	batch.Query(
		`UPDATE inventory_totals
		 SET total_available = ?, total_reserved = ?, last_event_timestamp = ?
		 WHERE product_id = ?`,
		totals.Available, totals.Reserved, totals.LastEvent, productID,
	)
}

func (h *Handler) putHistory(batch *gocql.Batch, productID string, event *pb.WarehouseEvent) {
	payload, err := protojson.Marshal(event)
	if err != nil {
		payload = []byte(`{"marshal_error":true}`)
	}
	batch.Query(
		`INSERT INTO event_history (product_id, event_timestamp, event_id, event_type, payload)
		 VALUES (?, ?, ?, ?, ?)`,
		productID, eventTime(event), event.EventId, event.EventType, string(payload),
	)
}

func (h *Handler) isOldZoneEvent(ctx context.Context, current time.Time, productID, zoneID string) (bool, error) {
	last, err := h.store.GetLastEventTimestamp(ctx, productID, zoneID)
	if err != nil {
		return false, err
	}
	if !last.IsZero() && !current.After(last) {
		h.log.Info("out-of-order skipped", "product_id", productID, "zone_id", zoneID, "event_timestamp", current, "last_event_timestamp", last)
		return true, nil
	}
	return false, nil
}

func (h *Handler) isOldOrderEvent(ctx context.Context, current time.Time, orderID string) (bool, error) {
	order, err := h.store.GetOrder(ctx, orderID)
	if errors.Is(err, gocql.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !order.LastEvent.IsZero() && !current.After(order.LastEvent) {
		h.log.Info("out-of-order order skipped", "order_id", orderID, "event_timestamp", current, "last_event_timestamp", order.LastEvent)
		return true, nil
	}
	return false, nil
}

func validateProductZoneQty(productID, zoneID string, quantity int64) error {
	if productID == "" || zoneID == "" {
		return validationError("product_id and zone_id are required")
	}
	if quantity <= 0 {
		return validationError("invalid quantity %d", quantity)
	}
	return nil
}

func normalizeOrderItems(pbItems []*pb.OrderItem) ([]orderItem, error) {
	if len(pbItems) == 0 {
		return nil, validationError("order must contain at least one item")
	}
	byKey := map[string]orderItem{}
	for _, item := range pbItems {
		if err := validateProductZoneQty(item.GetProductId(), item.GetZoneId(), int64(item.GetQuantity())); err != nil {
			return nil, err
		}
		key := item.GetProductId() + "\x00" + item.GetZoneId()
		current := byKey[key]
		current.ProductID = item.GetProductId()
		current.ZoneID = item.GetZoneId()
		current.Quantity += int64(item.GetQuantity())
		byKey[key] = current
	}
	items := make([]orderItem, 0, len(byKey))
	for _, item := range byKey {
		items = append(items, item)
	}
	return items, nil
}

func eventTime(event *pb.WarehouseEvent) time.Time {
	return time.UnixMilli(event.Timestamp).UTC()
}
