import time
import re
import random
import threading
from typing import Any, Callable, Optional, List, Dict, Pattern

from core import (
    Stage, Source, LogEvent, LogLevel
)


class MockLogSource(Source):
    def __init__(self, name: str, event_count: int = 100, rate_per_sec: float = 10.0,
                 out_of_order_prob: float = 0.1, max_out_of_order_sec: float = 5.0):
        super().__init__(name)
        self.event_count = event_count
        self.rate_per_sec = rate_per_sec
        self.out_of_order_prob = out_of_order_prob
        self.max_out_of_order_sec = max_out_of_order_sec
        self.sources = ["web-server-01", "web-server-02", "api-gateway", "db-master", "db-replica"]
        self.messages = {
            LogLevel.DEBUG: ["Processing request id={req_id}", "Cache hit for key={key}", "Connection pool size={size}"],
            LogLevel.INFO: ["Request completed in {latency}ms", "User {user} logged in", "Service started successfully"],
            LogLevel.WARN: ["High memory usage: {pct}%", "Slow query detected: {sql}", "Retrying operation, attempt={n}"],
            LogLevel.ERROR: ["Connection refused to {host}:{port}", "Null pointer in {module}", "Timeout after {sec}s"],
            LogLevel.FATAL: ["Out of memory error", "Disk full on {path}", "Database crash detected"]
        }

    def generate(self):
        base_time = time.time()
        interval = 1.0 / self.rate_per_sec
        pending_events: List[LogEvent] = []

        for i in range(self.event_count):
            if not self._running:
                break

            while self._should_slow_down() and self._running:
                time.sleep(self._get_sleep_time())

            event_time = base_time + i * interval
            level = random.choices(
                [LogLevel.DEBUG, LogLevel.INFO, LogLevel.WARN, LogLevel.ERROR, LogLevel.FATAL],
                weights=[40, 35, 15, 8, 2]
            )[0]
            msg_template = random.choice(self.messages[level])
            fields = {
                "req_id": f"req-{i:06d}",
                "key": f"cache-{i % 100}",
                "size": random.randint(5, 100),
                "latency": random.randint(1, 2000),
                "user": f"user_{random.randint(1, 1000)}",
                "pct": random.randint(70, 95),
                "sql": f"SELECT * FROM table WHERE id={i}",
                "n": random.randint(1, 5),
                "host": random.choice(["10.0.0.1", "10.0.0.2", "10.0.0.3"]),
                "port": random.randint(1000, 9999),
                "module": random.choice(["auth", "payment", "search", "user"]),
                "sec": random.randint(1, 30),
                "path": random.choice(["/data", "/var/log", "/tmp"])
            }
            message = msg_template.format(**fields)
            fields["error_code"] = f"E{random.randint(1000, 9999)}" if level in (LogLevel.ERROR, LogLevel.FATAL) else None

            event = LogEvent(
                timestamp=event_time,
                level=level,
                message=message,
                source=random.choice(self.sources),
                fields=fields
            )

            if random.random() < self.out_of_order_prob:
                delay = random.uniform(0.1, self.max_out_of_order_sec)
                pending_events.append(event)
                for j in range(min(3, len(pending_events))):
                    if random.random() < 0.5:
                        idx = random.randint(0, len(pending_events) - 1)
                        late_event = pending_events.pop(idx)
                        self.emit(late_event)
            else:
                self.emit(event)

            if i % 50 == 0:
                while pending_events:
                    self.emit(pending_events.pop(0))

            time.sleep(interval)

        for e in pending_events:
            self.emit(e)

        time.sleep(2)
        self._running = False


class ParseStage(Stage):
    def __init__(self, name: str = "parse"):
        super().__init__(name)
        self.patterns: List[Pattern] = [
            re.compile(r'(?P<timestamp>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})'),
            re.compile(r'\[(?P<level>DEBUG|INFO|WARN|WARNING|ERROR|FATAL)\]'),
            re.compile(r'\[source=(?P<source>[^\]]+)\]')
        ]

    def process(self, event: Any) -> Optional[LogEvent]:
        if isinstance(event, LogEvent):
            return event
        if isinstance(event, str):
            return self._parse_from_string(event)
        if isinstance(event, dict):
            return self._parse_from_dict(event)
        return None

    def _parse_from_string(self, line: str) -> Optional[LogEvent]:
        try:
            level = LogLevel.INFO
            timestamp = time.time()
            source = "unknown"
            for p in self.patterns:
                m = p.search(line)
                if m:
                    gd = m.groupdict()
                    if "level" in gd:
                        lv = gd["level"].upper()
                        if lv == "WARNING":
                            lv = "WARN"
                        level = LogLevel[lv]
                    if "timestamp" in gd:
                        try:
                            import datetime
                            ts_str = gd["timestamp"].replace("T", " ")
                            timestamp = datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").timestamp()
                        except:
                            pass
                    if "source" in gd:
                        source = gd["source"]
            return LogEvent(
                timestamp=timestamp,
                level=level,
                message=line,
                source=source
            )
        except:
            return None

    def _parse_from_dict(self, data: dict) -> Optional[LogEvent]:
        try:
            level_name = data.get("level", "INFO").upper()
            if level_name == "WARNING":
                level_name = "WARN"
            return LogEvent(
                timestamp=data.get("timestamp", time.time()),
                level=LogLevel[level_name],
                message=data.get("message", ""),
                source=data.get("source", "unknown"),
                fields=data.get("fields", {})
            )
        except:
            return None


class FilterStage(Stage):
    def __init__(self, name: str = "filter",
                 predicate: Optional[Callable[[LogEvent], bool]] = None,
                 min_level: Optional[LogLevel] = None):
        super().__init__(name)
        self.predicate = predicate
        self.min_level = min_level

    def process(self, event: Any) -> Optional[LogEvent]:
        if not isinstance(event, LogEvent):
            return None
        if self.min_level is not None and event.level.value < self.min_level.value:
            return None
        if self.predicate is not None and not self.predicate(event):
            return None
        return event


class TransformStage(Stage):
    def __init__(self, name: str = "transform",
                 func: Optional[Callable[[LogEvent], LogEvent]] = None):
        super().__init__(name)
        self.func = func

    def process(self, event: Any) -> Optional[LogEvent]:
        if not isinstance(event, LogEvent):
            return None
        if self.func:
            return self.func(event)
        return event


class ConsoleSink(Stage):
    def __init__(self, name: str = "console_sink", format_func: Optional[Callable] = None):
        super().__init__(name)
        self.format_func = format_func
        self.received: List[Any] = []

    def process(self, event: Any) -> Optional[Any]:
        self.received.append(event)
        if self.format_func:
            print(self.format_func(event))
        else:
            if isinstance(event, LogEvent):
                ts = time.strftime("%H:%M:%S", time.localtime(event.timestamp))
                print(f"[{ts}] [{event.level.name}] [{event.source}] {event.message}")
            elif hasattr(event, '__dict__'):
                print(event)
            else:
                print(event)
        return None


class SlowSink(Stage):
    def __init__(self, name: str = "slow_sink", delay_per_event: float = 0.2):
        super().__init__(name)
        self.delay_per_event = delay_per_event
        self.received: List[Any] = []

    def process(self, event: Any) -> Optional[Any]:
        self.received.append(event)
        time.sleep(self.delay_per_event)
        return None
