Ahmed Early Explosion PRO ANALYST v4 - SELL QUALITY FIX

Changes based on stats(17):
- BUY EARLY and BUY EXPLOSION logic preserved.
- Normal SELL EARLY is internal/watch-only by default (no Telegram alert).
- SELL CONFIRMED now requires buyer failure: rejection + live micro structure + reclaim/flow + proximity to 4H sell zone.
- SELL EXPLOSION and live BUY<->SELL reversal engine remain active.
- 4H remains context/location; 5M/15M are live timing triggers.
- Structural stop + ATR padding and Anti-Late remain active.

Optional env:
SELL_EARLY_ALERTS=false
SELL_CONFIRMED_REJECTION_MIN=65
SELL_CONFIRMED_FLOW_MIN=58
SELL_CONFIRMED_TRIGGER_MIN=57
SELL_CONFIRMED_ZONE_MAX_ATR4=0.22
