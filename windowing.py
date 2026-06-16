import time
import threading
from typing import Any, Callable, Optional, List, Dict, Tuple
from collections import defaultdict, OrderedDict
from dataclasses import dataclass, field

from core import (
    Stage, LogEvent, LogLevel, Window, AggregationResult,
    WindowType, LateEventStrategy
)


@dataclass
class WatermarkState:
    current_watermark: float = 0.0
    max_event_time: float = 0.0
    out_of_orderness: float = 5.0
    last_advanced_at: float = field(default_factory=time.time)


class WindowAssigner:
    def assign_windows(self, timestamp: float) -> List[Window]:
        raise NotImplementedError


class TumblingWindowAssigner(WindowAssigner):
    def __init__(self, size_seconds: float):
        self.size = size_seconds

    def assign_windows(self, timestamp: float) -> List[Window]:
        start = int(timestamp / self.size) * self.size
        end = start + self.size
        return [Window(start=float(start), end=float(end))]


class SlidingWindowAssigner(WindowAssigner):
    def __init__(self, size_seconds: float, slide_seconds: float):
        self.size = size_seconds
        self.slide = slide_seconds

    def assign_windows(self, timestamp: float) -> List[Window]:
        windows = []
        start = int((timestamp - self.size) / self.slide) * self.slide
        while start <= timestamp:
            end = start + self.size
            if start <= timestamp < end:
                windows.append(Window(start=float(start), end=float(end)))
            start += self.slide
        return windows


class SessionWindowAssigner(WindowAssigner):
    def __init__(self, gap_seconds: float):
        self.gap = gap_seconds

    def assign_windows(self, timestamp: float) -> List[Window]:
        return [Window(start=timestamp, end=timestamp + self.gap)]


class Aggregator:
    def create(self) -> Dict[str, Any]:
        raise NotImplementedError

    def add(self, state: Dict[str, Any], event: LogEvent) -> Dict[str, Any]:
        raise NotImplementedError

    def result(self, state: Dict[str, Any]) -> Dict[str, Any]:
        return state


class CountAggregator(Aggregator):
    def create(self) -> Dict[str, Any]:
        return {"count": 0}

    def add(self, state: Dict[str, Any], event: LogEvent) -> Dict[str, Any]:
        state["count"] += 1
        return state


class LevelCountAggregator(Aggregator):
    def create(self) -> Dict[str, Any]:
        return {"total": 0, "error_count": 0, "fatal_count": 0, "warn_count": 0,
                "levels": defaultdict(int)}

    def add(self, state: Dict[str, Any], event: LogEvent) -> Dict[str, Any]:
        state["total"] += 1
        state["levels"][event.level.name] += 1
        if event.level == LogLevel.ERROR:
            state["error_count"] += 1
        elif event.level == LogLevel.FATAL:
            state["fatal_count"] += 1
        elif event.level == LogLevel.WARN:
            state["warn_count"] += 1
        return state

    def result(self, state: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "total": state["total"],
            "error_count": state["error_count"],
            "fatal_count": state["fatal_count"],
            "warn_count": state["warn_count"],
            "error_rate": state["error_count"] / max(1, state["total"]),
            "levels": dict(state["levels"])
        }


class ErrorRateAggregator(Aggregator):
    def create(self) -> Dict[str, Any]:
        return {"total": 0, "errors": 0}

    def add(self, state: Dict[str, Any], event: LogEvent) -> Dict[str, Any]:
        state["total"] += 1
        if event.level in (LogLevel.ERROR, LogLevel.FATAL):
            state["errors"] += 1
        return state

    def result(self, state: Dict[str, Any]) -> Dict[str, Any]:
        total = max(1, state["total"])
        return {
            "total": state["total"],
            "errors": state["errors"],
            "error_rate": state["errors"] / total
        }


class WindowAggregationStage(Stage):
    def __init__(self,
                 name: str = "window_agg",
                 window_type: WindowType = WindowType.TUMBLING,
                 window_size_seconds: float = 60.0,
                 window_slide_seconds: Optional[float] = None,
                 session_gap_seconds: float = 30.0,
                 allowed_lateness_seconds: float = 10.0,
                 out_of_orderness_seconds: float = 5.0,
                 aggregator: Optional[Aggregator] = None,
                 key_extractor: Optional[Callable[[LogEvent], Any]] = None,
                 late_event_strategy: LateEventStrategy = LateEventStrategy.DISCARD,
                 state_cleanup_interval_seconds: float = 60.0,
                 side_output: Optional[Stage] = None):
        super().__init__(name)
        self.window_type = window_type
        self.window_size = window_size_seconds
        self.window_slide = window_slide_seconds or window_size_seconds
        self.session_gap = session_gap_seconds
        self.allowed_lateness = allowed_lateness_seconds
        self.out_of_orderness = out_of_orderness_seconds
        self.aggregator = aggregator or CountAggregator()
        self.key_extractor = key_extractor
        self.late_strategy = late_event_strategy
        self.cleanup_interval = state_cleanup_interval_seconds
        self.side_output = side_output

        if window_type == WindowType.TUMBLING:
            self.window_assigner = TumblingWindowAssigner(window_size_seconds)
        elif window_type == WindowType.SLIDING:
            self.window_assigner = SlidingWindowAssigner(window_size_seconds, self.window_slide)
        else:
            self.window_assigner = SessionWindowAssigner(session_gap_seconds)

        self._watermark = WatermarkState(out_of_orderness=out_of_orderness_seconds)
        self._window_states: Dict[Tuple[Any, float, float], Dict[str, Any]] = {}
        self._window_metadata: OrderedDict[Tuple[Any, float, float], Dict[str, Any]] = OrderedDict()
        self._lock = threading.Lock()
        self._last_cleanup = time.time()
        self._late_events_count = 0
        self._fired_windows = 0

    def _extract_key(self, event: LogEvent) -> Any:
        if self.key_extractor:
            return self.key_extractor(event)
        return None

    def _update_watermark(self, event_time: float):
        with self._lock:
            if event_time > self._watermark.max_event_time:
                self._watermark.max_event_time = event_time
                new_wm = event_time - self.out_of_orderness
                if new_wm > self._watermark.current_watermark:
                    self._watermark.current_watermark = new_wm
                    self._watermark.last_advanced_at = time.time()

    def _get_watermark(self) -> float:
        with self._lock:
            return self._watermark.current_watermark

    def _is_late(self, event: LogEvent) -> bool:
        watermark = self._get_watermark()
        return event.event_time() < (watermark - self.allowed_lateness)

    def _is_window_ready(self, window: Window) -> bool:
        watermark = self._get_watermark()
        return watermark >= window.end

    def _is_window_expired(self, window: Window) -> bool:
        watermark = self._get_watermark()
        return watermark >= (window.end + self.allowed_lateness)

    def _merge_session_windows(self, key: Any, new_window: Window) -> Window:
        if self.window_type != WindowType.SESSION:
            return new_window

        merged = Window(start=new_window.start, end=new_window.end, key=key)
        to_remove = []

        for (w_key, w_start, w_end), meta in self._window_metadata.items():
            if w_key != key:
                continue
            existing = Window(start=w_start, end=w_end, key=w_key)
            if (existing.start <= merged.end + self.session_gap and
                    merged.start <= existing.end + self.session_gap):
                merged.start = min(merged.start, existing.start)
                merged.end = max(merged.end, existing.end)
                to_remove.append((w_key, w_start, w_end))

        for k in to_remove:
            if k in self._window_states:
                old_state = self._window_states.pop(k)
                state_key = (key, merged.start, merged.end)
                if state_key in self._window_states:
                    pass
                else:
                    self._window_states[state_key] = old_state
                self._window_metadata.pop(k, None)

        return merged

    def process(self, event: Any) -> Optional[List[AggregationResult]]:
        if not isinstance(event, LogEvent):
            return None

        self._update_watermark(event.event_time())

        if self._is_late(event):
            return self._handle_late_event(event)

        results: List[AggregationResult] = []
        key = self._extract_key(event)
        assigned_windows = self.window_assigner.assign_windows(event.event_time())

        for win in assigned_windows:
            win.key = key

            if self.window_type == WindowType.SESSION:
                win = self._merge_session_windows(key, win)

            state_key = (key, win.start, win.end)

            with self._lock:
                if state_key not in self._window_states:
                    self._window_states[state_key] = self.aggregator.create()
                    self._window_metadata[state_key] = {
                        "created_at": time.time(),
                        "last_updated": time.time(),
                        "fired": False
                    }

                self._window_states[state_key] = self.aggregator.add(
                    self._window_states[state_key], event
                )
                self._window_metadata[state_key]["last_updated"] = time.time()

            if self._is_window_ready(win):
                fired = self._try_fire_window(win, key)
                if fired:
                    results.append(fired)

        fired_from_wm = self._fire_ready_windows()
        results.extend(fired_from_wm)

        self._maybe_cleanup()

        return results if results else None

    def _handle_late_event(self, event: LogEvent) -> Optional[List[AggregationResult]]:
        self._late_events_count += 1

        if self.late_strategy == LateEventStrategy.DISCARD:
            self.metrics["dropped"] += 1
            return None

        key = self._extract_key(event)
        assigned_windows = self.window_assigner.assign_windows(event.event_time())
        results: List[AggregationResult] = []

        for win in assigned_windows:
            win.key = key
            state_key = (key, win.start, win.end)

            with self._lock:
                if self._is_window_expired(win):
                    self.metrics["dropped"] += 1
                    if self.side_output and self.late_strategy == LateEventStrategy.SIDE_OUTPUT:
                        try:
                            self.side_output.input_queue.put(event, timeout=0.01)
                        except:
                            pass
                    continue

                if state_key not in self._window_states:
                    self._window_states[state_key] = self.aggregator.create()
                    self._window_metadata[state_key] = {
                        "created_at": time.time(),
                        "last_updated": time.time(),
                        "fired": True
                    }

                self._window_states[state_key] = self.aggregator.add(
                    self._window_states[state_key], event
                )
                self._window_metadata[state_key]["last_updated"] = time.time()

            if self.late_strategy == LateEventStrategy.UPDATE:
                fired = self._try_fire_window(win, key, is_late_update=True)
                if fired:
                    results.append(fired)

        return results if results else None

    def _try_fire_window(self, window: Window, key: Any,
                         is_late_update: bool = False) -> Optional[AggregationResult]:
        state_key = (key, window.start, window.end)

        with self._lock:
            if state_key not in self._window_states:
                return None
            state = self._window_states[state_key]
            meta = self._window_metadata.get(state_key, {})

            if not is_late_update and meta.get("fired", False):
                return None

            meta["fired"] = True
            result = self.aggregator.result(dict(state))

        self._fired_windows += 1
        return AggregationResult(
            window=window,
            aggregations=result,
            is_late_update=is_late_update
        )

    def _fire_ready_windows(self) -> List[AggregationResult]:
        results: List[AggregationResult] = []
        wm = self._get_watermark()

        keys_to_check = list(self._window_metadata.keys())
        for state_key in keys_to_check:
            key, w_start, w_end = state_key
            window = Window(start=w_start, end=w_end, key=key)

            if wm >= window.end:
                meta = self._window_metadata.get(state_key, {})
                if not meta.get("fired", False):
                    fired = self._try_fire_window(window, key)
                    if fired:
                        results.append(fired)

        return results

    def _maybe_cleanup(self):
        now = time.time()
        if now - self._last_cleanup < self.cleanup_interval:
            return

        self._last_cleanup = now
        wm = self._get_watermark()
        cleaned = 0

        with self._lock:
            keys_to_remove = []
            for state_key, meta in self._window_metadata.items():
                key, w_start, w_end = state_key
                window_end = w_end + self.allowed_lateness

                if wm >= window_end:
                    idle_time = now - meta.get("last_updated", now)
                    if idle_time > self.cleanup_interval:
                        keys_to_remove.append(state_key)

            for k in keys_to_remove:
                self._window_states.pop(k, None)
                self._window_metadata.pop(k, None)
                cleaned += 1

        if cleaned > 0:
            pass

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "watermark": self._watermark.current_watermark,
                "max_event_time": self._watermark.max_event_time,
                "active_windows": len(self._window_states),
                "fired_windows": self._fired_windows,
                "late_events": self._late_events_count,
                "processed": self.metrics["processed"]
            }
