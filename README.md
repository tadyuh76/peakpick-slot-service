# PeakPick Slot Service

Slot Service là microservice quản lý khung giờ nhận hàng, sức chứa và việc gán ô pickup cho đơn đã thanh toán.

## Database Riêng

Service này sở hữu database `peakpick_slot` với các bảng:

- `pickup_windows`
- `pickup_slots`
- `slot_reservations`
- `slot_reservation_blocks`
- `event_log`

## Event

Nhận event:

- `OrderPaid`
- `OrderPickedUp`
- `OrderExpired`

Phát event:

- `PickupSlotReserved`
- `PickupSlotFull`

## Chạy Local

```bash
pip install -r requirements.txt
uvicorn services.slot_service.main:app --reload --port 8003
```
