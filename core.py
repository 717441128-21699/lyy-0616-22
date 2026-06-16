import time
import threading
import queue
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, List, Dict, Tuple
from enum import Enum
from collections import deque


class LogLevel(Enum):
    DEBUG = 0
    INFO = 1
    WARN = 2
    ERROR = 3
    FATAL = 4


class LateEventStrategy(Enum):
    DISCARD = "discard"
    UPDATE = "update"
    SIDE_OUTPUT = "side_output"


class WindowType(Enum):
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"


@dataclass
class LogEvent:
    timestamp: float
    level: LogLevel
    message: str
    source: str
    fields: Dict[str, Any] = field(default_factory=dict)
    ingestion_time: float = field(default_factory=time.time)

    def event_time(self) -> float:
        return self.timestamp


@dataclass
class Window:
    start: float
    end: float
    key: Any = None
    state: Dict[str, Any] = field(default_factory=dict)
    is_closed: bool = False
    fired: bool = False

    def contains(self, timestamp: float) -> bool:
        return self.start <= timestamp < self.end

    def __hash__(self):
        return hash((self.start, self.end, str(self.key)))

    def __eq__(self, other):
        if not isinstance(other, Window):
            return False
        return self.start == other.start and self.end == other.end and self.key == other.key


@dataclass
class AggregationResult:
    window: Window
    aggregations: Dict[str, Any]
    is_late_update: bool = False
    created_at: float = field(default_factory=time.time)


@dataclass
class Alert:
    rule_id: str
    severity: LogLevel
    message: str
    window: Optional[Window] = None
    metric_value: Any = None
    threshold: Any = None
    created_at: float = field(default_factory=time.time)


class BackPressureSignal(Enum):
    GREEN = 0
    YELLOW = 1
    RED = 2


@dataclass
class BackPressureState:
    signal: BackPressureSignal = BackPressureSignal.GREEN
    queue_size: int = 0
    threshold_yellow: int = 100
    threshold_red: int = 500
    last_updated: float = field(default_factory=time.time)


class Stage:
    def __init__(self, name: str):
        self.name = name
        self.next_stages: List["Stage"] = []
        self.input_queue: queue.Queue = queue.Queue(maxsize=1000)
        self.backpressure_state = BackPressureState()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self.metrics = {
            "processed": 0,
            "dropped": 0,
            "backpressure_events": 0
        }

    def connect(self, next_stage: "Stage") -> "Stage":
        self.next_stages.append(next_stage)
        return next_stage

    def process(self, event: Any) -> Optional[Any]:
        raise NotImplementedError

    def emit(self, event: Any):
        if event is None:
            return
        for next_stage in self.next_stages:
            try:
                next_stage.input_queue.put(event, timeout=0.1)
            except queue.Full:
                self.metrics["dropped"] += 1
                self._signal_backpressure(next_stage)
                continue

    def _signal_backpressure(self, next_stage: "Stage"):
        with self._lock:
            bp_state = next_stage.backpressure_state
            qsize = next_stage.input_queue.qsize()
            bp_state.queue_size = qsize
            if qsize >= bp_state.threshold_red:
                bp_state.signal = BackPressureSignal.RED
            elif qsize >= bp_state.threshold_yellow:
                bp_state.signal = BackPressureSignal.YELLOW
            else:
                bp_state.signal = BackPressureSignal.GREEN
            bp_state.last_updated = time.time()
            self.metrics["backpressure_events"] += 1

    def get_backpressure_signal(self) -> BackPressureSignal:
        if not self.next_stages:
            return BackPressureSignal.GREEN
        max_signal = BackPressureSignal.GREEN
        for s in self.next_stages:
            if s.backpressure_state.signal.value > max_signal.value:
                max_signal = s.backpressure_state.signal
        return max_signal

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self):
        while self._running:
            try:
                event = self.input_queue.get(timeout=0.05)
                result = self.process(event)
                if result is not None:
                    if isinstance(result, list):
                        for r in result:
                            self.emit(r)
                    else:
                        self.emit(result)
                self.metrics["processed"] += 1
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[{self.name}] Error: {e}")


class Source(Stage):
    def __init__(self, name: str):
        super().__init__(name)
        self._stop_event = threading.Event()

    def process(self, event: Any) -> Optional[Any]:
        return event

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run_source, daemon=True)
        self._thread.start()

    def _run_source(self):
        self.generate()

    def generate(self):
        raise NotImplementedError

    def _should_slow_down(self) -> bool:
        signal = self.get_backpressure_signal()
        return signal in (BackPressureSignal.YELLOW, BackPressureSignal.RED)

    def _get_sleep_time(self) -> float:
        signal = self.get_backpressure_signal()
        if signal == BackPressureSignal.RED:
            return 0.5
        elif signal == BackPressureSignal.YELLOW:
            return 0.1
        return 0.0
