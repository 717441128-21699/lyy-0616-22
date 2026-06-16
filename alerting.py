import time
import threading
from typing import Any, Callable, Optional, List, Dict, Union
from dataclasses import dataclass, field

from core import Stage, LogEvent, LogLevel, Alert, AggregationResult, Window


@dataclass
class AlertRule:
    rule_id: str
    severity: LogLevel
    condition: Callable[[Any], bool]
    message_template: str
    cooldown_seconds: float = 60.0
    _last_fired: float = 0.0

    def should_fire(self, value: Any) -> bool:
        now = time.time()
        if now - self._last_fired < self.cooldown_seconds:
            return False
        if self.condition(value):
            self._last_fired = now
            return True
        return False

    def format_message(self, **kwargs) -> str:
        return self.message_template.format(**kwargs)


class ThresholdAlert:
    def __init__(self, rule_id: str, threshold: float,
                 severity: LogLevel = LogLevel.WARN,
                 op: str = ">",
                 metric_name: str = "value",
                 message: Optional[str] = None,
                 cooldown_seconds: float = 60.0):
        self.rule_id = rule_id
        self.threshold = threshold
        self.severity = severity
        self.op = op
        self.metric_name = metric_name
        self.cooldown = cooldown_seconds
        self._last_fired = 0.0
        self.message = message or f"{metric_name} {op} {threshold}"

    def _compare(self, value: float) -> bool:
        ops = {
            ">": lambda a, b: a > b,
            ">=": lambda a, b: a >= b,
            "<": lambda a, b: a < b,
            "<=": lambda a, b: a <= b,
            "==": lambda a, b: a == b,
            "!=": lambda a, b: a != b,
        }
        return ops.get(self.op, ops[">"])(value, self.threshold)

    def check(self, aggregations: Dict[str, Any]) -> Optional[Alert]:
        value = aggregations.get(self.metric_name)
        if value is None:
            return None

        now = time.time()
        if now - self._last_fired < self.cooldown:
            return None

        try:
            if not self._compare(float(value)):
                return None
        except (TypeError, ValueError):
            return None

        self._last_fired = now
        return Alert(
            rule_id=self.rule_id,
            severity=self.severity,
            message=f"{self.message}: actual={value}",
            metric_value=value,
            threshold=self.threshold
        )


class AlertStage(Stage):
    def __init__(self, name: str = "alert",
                 rules: Optional[List[ThresholdAlert]] = None,
                 alert_callback: Optional[Callable[[Alert], None]] = None):
        super().__init__(name)
        self.rules: List[ThresholdAlert] = rules or []
        self.alert_callback = alert_callback
        self.fired_alerts: List[Alert] = []
        self._lock = threading.Lock()

    def add_rule(self, rule: ThresholdAlert):
        self.rules.append(rule)

    def process(self, event: Any) -> Optional[Alert]:
        if isinstance(event, AggregationResult):
            return self._process_aggregation(event)
        elif isinstance(event, LogEvent):
            return self._process_log_event(event)
        return None

    def _process_aggregation(self, agg: AggregationResult) -> Optional[Alert]:
        alerts: List[Alert] = []
        for rule in self.rules:
            alert = rule.check(agg.aggregations)
            if alert:
                alert.window = agg.window
                alerts.append(alert)

        if alerts:
            for a in alerts:
                with self._lock:
                    self.fired_alerts.append(a)
                if self.alert_callback:
                    self.alert_callback(a)
            return alerts if len(alerts) > 1 else alerts[0]
        return None

    def _process_log_event(self, event: LogEvent) -> Optional[Alert]:
        if event.level in (LogLevel.ERROR, LogLevel.FATAL):
            for rule in self.rules:
                if rule.metric_name == "level":
                    alert = rule.check({"level": event.level.value})
                    if alert:
                        alert.message = f"[{event.source}] {event.message}"
                        with self._lock:
                            self.fired_alerts.append(alert)
                        if self.alert_callback:
                            self.alert_callback(alert)
                        return alert
        return None

    def get_alerts(self) -> List[Alert]:
        with self._lock:
            return list(self.fired_alerts)


class AggregationResultFormatter(Stage):
    def __init__(self, name: str = "agg_formatter"):
        super().__init__(name)

    def process(self, event: Any) -> Optional[str]:
        if not isinstance(event, AggregationResult):
            return None

        win = event.window
        start_str = time.strftime("%H:%M:%S", time.localtime(win.start))
        end_str = time.strftime("%H:%M:%S", time.localtime(win.end))
        key_str = f" key={win.key}" if win.key is not None else ""
        late_str = " [LATE UPDATE]" if event.is_late_update else ""

        agg_parts = []
        for k, v in event.aggregations.items():
            if isinstance(v, float):
                agg_parts.append(f"{k}={v:.4f}")
            elif isinstance(v, dict):
                agg_parts.append(f"{k}={dict(v)}")
            else:
                agg_parts.append(f"{k}={v}")

        return (f"[Window {start_str}~{end_str}{key_str}]{late_str} "
                f"{' | '.join(agg_parts)}")


class AlertFormatter(Stage):
    def __init__(self, name: str = "alert_formatter"):
        super().__init__(name)

    def process(self, event: Any) -> Optional[str]:
        if not isinstance(event, Alert):
            return None
        ts = time.strftime("%H:%M:%S", time.localtime(event.created_at))
        win_str = ""
        if event.window:
            s = time.strftime("%H:%M:%S", time.localtime(event.window.start))
            e = time.strftime("%H:%M:%S", time.localtime(event.window.end))
            win_str = f" [window {s}~{e}]"
        return (f"*** ALERT [{ts}] [{event.severity.name}] [{event.rule_id}]{win_str} "
                f"{event.message} (threshold={event.threshold}, value={event.metric_value})")
