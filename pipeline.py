import time
import threading
import json
from typing import Any, Callable, Optional, List, Dict, Union
from dataclasses import dataclass, field, asdict

from core import (
    Stage, Source, LogEvent, LogLevel, BackPressureSignal,
    LateEventStrategy, WindowType
)
from stages import (
    MockLogSource, ParseStage, FilterStage, TransformStage,
    ConsoleSink, SlowSink
)
from windowing import (
    WindowAggregationStage, CountAggregator, LevelCountAggregator,
    ErrorRateAggregator, Aggregator
)
from alerting import AlertStage, ThresholdAlert, AggregationResultFormatter, AlertFormatter


class Pipeline:
    def __init__(self, name: str = "log_analysis_pipeline"):
        self.name = name
        self.sources: List[Source] = []
        self.stages: List[Stage] = []
        self._all_stages: List[Stage] = []
        self._running = False
        self._monitor_thread: Optional[threading.Thread] = None

    def add_source(self, source: Source) -> Source:
        self.sources.append(source)
        self._all_stages.append(source)
        return source

    def add_stage(self, stage: Stage) -> Stage:
        self.stages.append(stage)
        self._all_stages.append(stage)
        return stage

    def start(self):
        print(f"[{self.name}] Starting pipeline with {len(self.sources)} sources "
              f"and {len(self.stages)} stages...")
        self._running = True

        for stage in self._all_stages:
            if not isinstance(stage, Source):
                stage.start()

        for source in self.sources:
            source.start()

        self._monitor_thread = threading.Thread(target=self._monitor, daemon=True)
        self._monitor_thread.start()

    def stop(self):
        print(f"[{self.name}] Stopping pipeline...")
        self._running = False
        for source in self.sources:
            source.stop()
        for stage in self._all_stages:
            stage.stop()

    def wait_until_complete(self, timeout: Optional[float] = None):
        start = time.time()
        while self._running:
            any_running = False
            for s in self.sources:
                if s._running and s._thread and s._thread.is_alive():
                    any_running = True
                    break
            if not any_running:
                time.sleep(1)
                break
            if timeout and (time.time() - start) > timeout:
                break
            time.sleep(0.1)
        self.stop()

    def _monitor(self):
        while self._running:
            time.sleep(5)
            if not self._running:
                break
            self._print_stats()

    def _print_stats(self):
        print("\n" + "=" * 60)
        print(f"[{self.name}] Pipeline Stats @ {time.strftime('%H:%M:%S')}")
        print("-" * 60)
        for stage in self._all_stages:
            bp = stage.backpressure_state
            bp_name = bp.signal.name
            qsize = stage.input_queue.qsize()
            extra = ""
            if hasattr(stage, 'get_stats'):
                stats = stage.get_stats()
                extra = f" | {stats}"
            print(f"  {stage.name:25s} | processed={stage.metrics['processed']:5d} "
                  f"| dropped={stage.metrics['dropped']:4d} | BP={bp_name:6s} "
                  f"| qsize={qsize:4d}{extra}")
        print("=" * 60 + "\n")

    def get_all_stats(self) -> Dict[str, Any]:
        stats = {}
        for stage in self._all_stages:
            stage_stats = dict(stage.metrics)
            stage_stats["backpressure"] = stage.backpressure_state.signal.name
            stage_stats["queue_size"] = stage.input_queue.qsize()
            if hasattr(stage, 'get_stats'):
                stage_stats.update(stage.get_stats())
            stats[stage.name] = stage_stats
        return stats


@dataclass
class PipelineConfig:
    source_event_count: int = 200
    source_rate_per_sec: float = 20.0
    source_out_of_order_prob: float = 0.2
    source_max_out_of_order_sec: float = 3.0
    filter_min_level: Optional[str] = None
    window_type: str = "tumbling"
    window_size_seconds: float = 5.0
    window_slide_seconds: Optional[float] = None
    allowed_lateness_seconds: float = 3.0
    out_of_orderness_seconds: float = 2.0
    late_strategy: str = "discard"
    aggregator_type: str = "level_count"
    key_by_source: bool = False
    error_count_threshold: int = 3
    error_rate_threshold: float = 0.1
    enable_slow_sink: bool = False
    slow_sink_delay: float = 0.3

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PipelineConfig":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


def build_pipeline_from_config(config: PipelineConfig) -> Pipeline:
    pipeline = Pipeline("configured_pipeline")

    source = MockLogSource(
        name="log_source",
        event_count=config.source_event_count,
        rate_per_sec=config.source_rate_per_sec,
        out_of_order_prob=config.source_out_of_order_prob,
        max_out_of_order_sec=config.source_max_out_of_order_sec
    )
    pipeline.add_source(source)

    parse = ParseStage(name="parse")
    pipeline.add_stage(parse)
    source.connect(parse)

    current = parse

    if config.filter_min_level:
        min_level = LogLevel[config.filter_min_level.upper()]
        filter_stage = FilterStage(name="filter_level", min_level=min_level)
        pipeline.add_stage(filter_stage)
        current.connect(filter_stage)
        current = filter_stage

    wtype = WindowType[config.window_type.upper()]
    late_strat = LateEventStrategy[config.late_strategy.upper()]

    if config.aggregator_type == "count":
        aggregator = CountAggregator()
    elif config.aggregator_type == "error_rate":
        aggregator = ErrorRateAggregator()
    else:
        aggregator = LevelCountAggregator()

    key_extractor = (lambda e: e.source) if config.key_by_source else None

    late_sink = None
    if late_strat == LateEventStrategy.SIDE_OUTPUT:
        late_sink = ConsoleSink(name="late_events_sink",
                                format_func=lambda e: f"  >> LATE: {e}")
        pipeline.add_stage(late_sink)

    window_agg = WindowAggregationStage(
        name="window_aggregation",
        window_type=wtype,
        window_size_seconds=config.window_size_seconds,
        window_slide_seconds=config.window_slide_seconds,
        allowed_lateness_seconds=config.allowed_lateness_seconds,
        out_of_orderness_seconds=config.out_of_orderness_seconds,
        aggregator=aggregator,
        key_extractor=key_extractor,
        late_event_strategy=late_strat,
        side_output=late_sink
    )
    pipeline.add_stage(window_agg)
    current.connect(window_agg)

    alert = AlertStage(name="alert_detector")
    alert.add_rule(ThresholdAlert(
        rule_id="error_count_high",
        threshold=config.error_count_threshold,
        severity=LogLevel.ERROR,
        op=">=",
        metric_name="error_count",
        message=f"Error count >= {config.error_count_threshold} per window",
        cooldown_seconds=10.0
    ))
    alert.add_rule(ThresholdAlert(
        rule_id="error_rate_high",
        threshold=config.error_rate_threshold,
        severity=LogLevel.WARN,
        op=">=",
        metric_name="error_rate",
        message=f"Error rate >= {config.error_rate_threshold:.0%}",
        cooldown_seconds=10.0
    ))
    pipeline.add_stage(alert)
    window_agg.connect(alert)

    agg_formatter = AggregationResultFormatter(name="agg_formatter")
    pipeline.add_stage(agg_formatter)
    window_agg.connect(agg_formatter)

    console = ConsoleSink(name="console_output")
    pipeline.add_stage(console)
    agg_formatter.connect(console)

    alert_formatter = AlertFormatter(name="alert_formatter")
    pipeline.add_stage(alert_formatter)
    alert.connect(alert_formatter)

    alert_console = ConsoleSink(name="alert_output")
    pipeline.add_stage(alert_console)
    alert_formatter.connect(alert_console)

    if config.enable_slow_sink:
        slow_sink = SlowSink(name="slow_sink", delay_per_event=config.slow_sink_delay)
        pipeline.add_stage(slow_sink)
        agg_formatter.connect(slow_sink)

    return pipeline
