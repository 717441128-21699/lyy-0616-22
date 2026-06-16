import time
import sys
import random

from core import (
    LogEvent, LogLevel, Window, AggregationResult, Alert,
    WindowType, LateEventStrategy, BackPressureSignal
)
from pipeline import Pipeline, PipelineConfig, build_pipeline_from_config
from stages import (
    MockLogSource, ParseStage, FilterStage, TransformStage,
    ConsoleSink, SlowSink
)
from windowing import (
    WindowAggregationStage, TumblingWindowAssigner,
    SlidingWindowAssigner, CountAggregator, LevelCountAggregator,
    ErrorRateAggregator
)
from alerting import AlertStage, ThresholdAlert


def demo_basic_pipeline():
    print("\n" + "=" * 70)
    print("DEMO 1: Basic Pipeline - Parse -> Filter -> Window Agg -> Alert")
    print("=" * 70 + "\n")

    config = PipelineConfig(
        source_event_count=150,
        source_rate_per_sec=15.0,
        source_out_of_order_prob=0.15,
        source_max_out_of_order_sec=2.0,
        window_type="tumbling",
        window_size_seconds=3.0,
        allowed_lateness_seconds=2.0,
        out_of_orderness_seconds=1.5,
        late_strategy="discard",
        aggregator_type="level_count",
        error_count_threshold=2,
        error_rate_threshold=0.15,
    )

    pipeline = build_pipeline_from_config(config)
    pipeline.start()
    pipeline.wait_until_complete(timeout=30)

    stats = pipeline.get_all_stats()
    print("\n--- Final Stats ---")
    for name, s in stats.items():
        print(f"  {name}: processed={s.get('processed', 'N/A')}, "
              f"dropped={s.get('dropped', 'N/A')}, "
              f"active_windows={s.get('active_windows', 'N/A')}, "
              f"late_events={s.get('late_events', 'N/A')}")


def demo_late_event_update():
    print("\n" + "=" * 70)
    print("DEMO 2: Late Event Strategy - UPDATE (re-fire windows with late data)")
    print("=" * 70 + "\n")

    config = PipelineConfig(
        source_event_count=80,
        source_rate_per_sec=10.0,
        source_out_of_order_prob=0.3,
        source_max_out_of_order_sec=4.0,
        window_type="tumbling",
        window_size_seconds=4.0,
        allowed_lateness_seconds=5.0,
        out_of_orderness_seconds=2.0,
        late_strategy="update",
        aggregator_type="count",
    )

    pipeline = build_pipeline_from_config(config)
    pipeline.start()
    pipeline.wait_until_complete(timeout=30)


def demo_side_output():
    print("\n" + "=" * 70)
    print("DEMO 3: Late Event Strategy - SIDE_OUTPUT (divert late events)")
    print("=" * 70 + "\n")

    config = PipelineConfig(
        source_event_count=100,
        source_rate_per_sec=12.0,
        source_out_of_order_prob=0.25,
        source_max_out_of_order_sec=3.0,
        window_type="tumbling",
        window_size_seconds=3.0,
        allowed_lateness_seconds=1.0,
        out_of_orderness_seconds=1.0,
        late_strategy="side_output",
        aggregator_type="level_count",
    )

    pipeline = build_pipeline_from_config(config)
    pipeline.start()
    pipeline.wait_until_complete(timeout=30)


def demo_backpressure():
    print("\n" + "=" * 70)
    print("DEMO 4: Backpressure - Slow sink causes upstream to slow down")
    print("=" * 70 + "\n")

    config = PipelineConfig(
        source_event_count=60,
        source_rate_per_sec=30.0,
        source_out_of_order_prob=0.05,
        source_max_out_of_order_sec=1.0,
        window_type="tumbling",
        window_size_seconds=2.0,
        allowed_lateness_seconds=1.0,
        out_of_orderness_seconds=1.0,
        late_strategy="discard",
        aggregator_type="count",
        enable_slow_sink=True,
        slow_sink_delay=0.25,
    )

    pipeline = build_pipeline_from_config(config)
    pipeline.start()
    pipeline.wait_until_complete(timeout=45)

    stats = pipeline.get_all_stats()
    print("\n--- Backpressure Analysis ---")
    slow_sink_stats = stats.get("slow_sink", {})
    source_stats = stats.get("log_source", {})
    print(f"  Slow sink processed: {slow_sink_stats.get('processed', 0)}")
    print(f"  Source backpressure events: {source_stats.get('backpressure_events', 0)}")
    print(f"  Total dropped due to BP: {sum(s.get('dropped', 0) for s in stats.values())}")


def demo_sliding_windows():
    print("\n" + "=" * 70)
    print("DEMO 5: Sliding Windows - Overlapping aggregation")
    print("=" * 70 + "\n")

    config = PipelineConfig(
        source_event_count=120,
        source_rate_per_sec=15.0,
        source_out_of_order_prob=0.1,
        source_max_out_of_order_sec=2.0,
        window_type="sliding",
        window_size_seconds=6.0,
        window_slide_seconds=2.0,
        allowed_lateness_seconds=2.0,
        out_of_orderness_seconds=1.5,
        late_strategy="discard",
        aggregator_type="error_rate",
        error_rate_threshold=0.1,
    )

    pipeline = build_pipeline_from_config(config)
    pipeline.start()
    pipeline.wait_until_complete(timeout=30)


def demo_keyed_aggregation():
    print("\n" + "=" * 70)
    print("DEMO 6: Keyed Aggregation - Group by source, per-server metrics")
    print("=" * 70 + "\n")

    config = PipelineConfig(
        source_event_count=150,
        source_rate_per_sec=15.0,
        source_out_of_order_prob=0.1,
        source_max_out_of_order_sec=2.0,
        window_type="tumbling",
        window_size_seconds=4.0,
        allowed_lateness_seconds=2.0,
        out_of_orderness_seconds=1.5,
        late_strategy="discard",
        aggregator_type="level_count",
        key_by_source=True,
        error_count_threshold=2,
    )

    pipeline = build_pipeline_from_config(config)
    pipeline.start()
    pipeline.wait_until_complete(timeout=30)


def demo_custom_pipeline():
    print("\n" + "=" * 70)
    print("DEMO 7: Custom Pipeline - Manual stage composition")
    print("=" * 70 + "\n")

    pipeline = Pipeline("custom_pipeline")

    source = MockLogSource(
        name="custom_source",
        event_count=100,
        rate_per_sec=12.0,
        out_of_order_prob=0.15,
        max_out_of_order_sec=2.0
    )
    pipeline.add_source(source)

    parse = ParseStage(name="parse")
    pipeline.add_stage(parse)
    source.connect(parse)

    filter_errors = FilterStage(
        name="filter_warn_above",
        min_level=LogLevel.WARN
    )
    pipeline.add_stage(filter_errors)
    parse.connect(filter_errors)

    enrich = TransformStage(
        name="enrich",
        func=lambda e: LogEvent(
            timestamp=e.timestamp,
            level=e.level,
            message=e.message,
            source=e.source,
            fields={**e.fields, "processed_by": "custom_pipeline",
                    "enriched": True}
        )
    )
    pipeline.add_stage(enrich)
    filter_errors.connect(enrich)

    window_agg = WindowAggregationStage(
        name="error_window_agg",
        window_type=WindowType.TUMBLING,
        window_size_seconds=5.0,
        allowed_lateness_seconds=3.0,
        out_of_orderness_seconds=2.0,
        aggregator=LevelCountAggregator(),
        late_event_strategy=LateEventStrategy.DISCARD
    )
    pipeline.add_stage(window_agg)
    enrich.connect(window_agg)

    console = ConsoleSink(name="custom_console")
    pipeline.add_stage(console)
    enrich.connect(console)

    alert = AlertStage(name="custom_alert")
    alert.add_rule(ThresholdAlert(
        rule_id="warn_plus_count",
        threshold=3,
        severity=LogLevel.WARN,
        op=">=",
        metric_name="warn_count",
        message="High warning count detected",
        cooldown_seconds=5.0
    ))
    pipeline.add_stage(alert)
    window_agg.connect(alert)

    from alerting import AggregationResultFormatter, AlertFormatter
    agg_fmt = AggregationResultFormatter(name="agg_fmt")
    pipeline.add_stage(agg_fmt)
    window_agg.connect(agg_fmt)

    agg_console = ConsoleSink(name="agg_output")
    pipeline.add_stage(agg_console)
    agg_fmt.connect(agg_console)

    alert_fmt = AlertFormatter(name="alert_fmt")
    pipeline.add_stage(alert_fmt)
    alert.connect(alert_fmt)

    alert_console = ConsoleSink(name="alert_out")
    pipeline.add_stage(alert_console)
    alert_fmt.connect(alert_console)

    pipeline.start()
    pipeline.wait_until_complete(timeout=30)


def run_all_demos():
    random.seed(42)

    print("\n" + "#" * 70)
    print("#  Stream Log Analysis Pipeline - Full Demo Suite")
    print("#" * 70)

    demos = [
        ("Basic Pipeline", demo_basic_pipeline),
        ("Late Event UPDATE", demo_late_event_update),
        ("Late Event SIDE_OUTPUT", demo_side_output),
        ("Backpressure", demo_backpressure),
        ("Sliding Windows", demo_sliding_windows),
        ("Keyed Aggregation", demo_keyed_aggregation),
        ("Custom Pipeline", demo_custom_pipeline),
    ]

    for i, (name, fn) in enumerate(demos, 1):
        try:
            fn()
        except Exception as e:
            print(f"\n!!! Demo '{name}' failed with error: {e}")
            import traceback
            traceback.print_exc()

        if i < len(demos):
            print(f"\n... Waiting 2s before next demo ...\n")
            time.sleep(2)

    print("\n" + "#" * 70)
    print("#  All demos completed!")
    print("#" * 70)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        demo_num = sys.argv[1]
        demo_map = {
            "1": demo_basic_pipeline,
            "2": demo_late_event_update,
            "3": demo_side_output,
            "4": demo_backpressure,
            "5": demo_sliding_windows,
            "6": demo_keyed_aggregation,
            "7": demo_custom_pipeline,
        }
        if demo_num in demo_map:
            random.seed(42)
            demo_map[demo_num]()
        else:
            print(f"Unknown demo number: {demo_num}")
            print(f"Available: 1-7")
    else:
        run_all_demos()
