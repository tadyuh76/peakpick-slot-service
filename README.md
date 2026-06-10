# PeakPick Slot Service

Owns pickup windows, slot capacity, slot assignment, and slot release.

Owned database tables:

- `pickup_windows`
- `pickup_slots`
- `slot_reservations`
- `slot_reservation_blocks`
- local `event_log`

Run locally:

```bash
pip install -r requirements.txt
uvicorn services.slot_service.main:app --reload --port 8003
```
