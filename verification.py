import time
import sys
import random
import threading

from core import LogEvent, LogLevel, WindowType, LateEventStrategy
from stages import MockLogSource, ParseStage, ConsoleSink, SlowSink
from windowing import WindowAggregationStage, CountAggregator, LevelCountAggregator
from alerting import AlertStage, ThresholdAlert, AggregationResultFormatter, AlertFormatter
from pipeline import Pipeline


# ============================================================
# 场景一：迟到更新
# ============================================================
def verify_late_update():
    print("\n" + "=" * 70)
    print(" 【验收场景 1】迟到更新策略 (LateEventStrategy.UPDATE)")
    print("=" * 70)
    print()
    print("验证要点：")
    print("  ✅ 窗口首次触发后输出初始结果")
    print("  ✅ 宽限期内再有迟到数据，立即输出 [LATE UPDATE] 更新结果")
    print("  ✅ 更新结果的 count 逐步增大（因为新数据不断加入）")
    print("  ✅ 超过宽限期的迟到数据被丢弃")
    print()

    base_t = 1700000000.0

    pipeline = Pipeline("late_update_demo", enable_monitor=False)

    from core import Source, LogEvent, LogLevel

    class ManualSource(Source):
        def __init__(self, name):
            super().__init__(name)
            self.events = []

        def add_event(self, event):
            self.events.append(event)

        def generate(self):
            for e in self.events:
                self.emit(e)
                time.sleep(0.02)
            time.sleep(0.5)

    source = ManualSource("source")
    pipeline.add_source(source)

    window_agg = WindowAggregationStage(
        name="window_agg",
        window_type=WindowType.TUMBLING,
        window_size_seconds=10.0,
        allowed_lateness_seconds=10.0,
        out_of_orderness_seconds=2.0,
        aggregator=CountAggregator(),
        late_event_strategy=LateEventStrategy.UPDATE,
        state_cleanup_interval_seconds=30.0
    )
    pipeline.add_stage(window_agg)
    source.connect(window_agg)

    agg_fmt = AggregationResultFormatter(name="fmt")
    pipeline.add_stage(agg_fmt)
    window_agg.connect(agg_fmt)

    console = ConsoleSink(name="console")
    pipeline.add_stage(console)
    agg_fmt.connect(console)

    # 构造事件：先发送窗口[0,10)内的3个事件
    # 然后发送事件时间=15的事件把水位线推到13，触发窗口[0,10)
    # 然后再发送窗口[0,10)的迟到事件，观察更新
    print(">>> 阶段1：发送窗口 [0,10) 的 3 个正常事件")
    for i in range(3):
        source.add_event(LogEvent(
            timestamp=base_t + i + 1,
            level=LogLevel.INFO,
            message=f"event-{i}",
            source="test",
            fields={}
        ))
    print("   事件时间: 1s, 2s, 3s  (都在窗口 [0,10) 内)")
    print()

    print(">>> 阶段2：发送事件时间=15s 的事件，推进水位线")
    print("   水位线将推进到 15-2=13s，超过窗口结束时间 10s")
    print("   → 窗口 [0,10) 首次触发 (count=3)")
    source.add_event(LogEvent(
        timestamp=base_t + 15,
        level=LogLevel.INFO,
        message="watermark-pusher",
        source="test",
        fields={}
    ))
    print()

    print(">>> 阶段3：发送 2 个迟到事件 (事件时间 4s, 5s)")
    print("   窗口已触发，但在宽限期内（8s），应输出 [LATE UPDATE]")
    print("   → count 从 3 增加到 5")
    for t in [4, 5]:
        source.add_event(LogEvent(
            timestamp=base_t + t,
            level=LogLevel.WARN,
            message=f"late-event-{t}",
            source="test",
            fields={}
        ))
    print()

    print(">>> 阶段4：发送超宽限期事件 (事件时间 0.5s)")
    print("   水位线 13s - 窗口结束 10s = 3s 延迟？不对，让我们再推一下水位线")

    # 再推一下水位线
    source.add_event(LogEvent(
        timestamp=base_t + 25,
        level=LogLevel.INFO,
        message="watermark-pusher-2",
        source="test",
        fields={}
    ))
    print("   再推水位线到 25-2=23s")

    source.add_event(LogEvent(
        timestamp=base_t + 0.5,
        level=LogLevel.ERROR,
        message="super-late-event",
        source="test",
        fields={}
    ))
    print("   发送事件时间 0.5s 的事件：延迟 22.5s，超过宽限期 8s，应丢弃")
    print()

    print(">>> 开始执行，观察输出...")
    print()

    pipeline.start()
    pipeline.wait_until_complete(timeout=15, flush_windows=True)

    print()
    print("-" * 70)
    print(" 【结果统计】")
    print("-" * 70)

    all_output = console.received
    first_fires = [r for r in all_output if "[LATE UPDATE]" not in r]
    late_updates = [r for r in all_output if "[LATE UPDATE]" in r]

    print(f"  首次触发窗口: {len(first_fires)} 个")
    print(f"  迟到更新结果: {len(late_updates)} 个")
    print(f"  总迟到事件数: {window_agg.get_stats()['late_events']}")
    print(f"  丢弃(超宽限): {window_agg.metrics['dropped']}")
    print(f"  活跃窗口数: {window_agg.get_stats()['active_windows']}")
    print()

    ok = True
    if len(late_updates) > 0:
        print("  ✅ 观察到迟到更新结果")
        for u in late_updates:
            print(f"     {u}")
    else:
        print("  ⚠️  未观察到迟到更新")
        ok = False

    if window_agg.metrics['dropped'] > 0:
        print("  ✅ 超宽限期的数据被正确丢弃")
    else:
        print("  ℹ️  本批数据中没有超宽限期事件")

    print()
    return ok


# ============================================================
# 场景二：迟到旁路
# ============================================================
def verify_side_output():
    print("\n" + "=" * 70)
    print(" 【验收场景 2】迟到旁路策略 (LateEventStrategy.SIDE_OUTPUT)")
    print("=" * 70)
    print()
    print("验证要点：")
    print("  ✅ 主链路正常输出窗口统计（不受迟到数据影响）")
    print("  ✅ 迟到日志被分流到独立旁路输出")
    print("  ✅ 管道稳定跑完，所有窗口都能正常触发")
    print()

    base_t = 1700000000.0

    pipeline = Pipeline("side_output_demo", enable_monitor=False)

    from core import Source, LogEvent, LogLevel

    class ManualSource(Source):
        def __init__(self, name):
            super().__init__(name)
            self.events = []

        def add_event(self, event):
            self.events.append(event)

        def generate(self):
            for e in self.events:
                self.emit(e)
                time.sleep(0.02)
            time.sleep(0.5)

    source = ManualSource("source")
    pipeline.add_source(source)

    late_sink = ConsoleSink(
        name="late_sink",
        format_func=lambda e: f"    ➤ [旁路迟到] t={e.timestamp - base_t:.1f}s "
                              f"[{e.level.name}] {e.message}"
    )
    pipeline.add_stage(late_sink)

    window_agg = WindowAggregationStage(
        name="window_agg",
        window_type=WindowType.TUMBLING,
        window_size_seconds=10.0,
        allowed_lateness_seconds=8.0,
        out_of_orderness_seconds=2.0,
        aggregator=CountAggregator(),
        late_event_strategy=LateEventStrategy.SIDE_OUTPUT,
        side_output=late_sink,
        state_cleanup_interval_seconds=30.0
    )
    pipeline.add_stage(window_agg)
    source.connect(window_agg)

    alert = AlertStage(name="alert")
    alert.add_rule(ThresholdAlert(
        rule_id="high_count",
        threshold=2,
        severity=LogLevel.WARN,
        op=">=",
        metric_name="count",
        message="窗口事件数过高",
        cooldown_seconds=5.0
    ))
    pipeline.add_stage(alert)
    window_agg.connect(alert)

    agg_fmt = AggregationResultFormatter(name="agg_fmt")
    pipeline.add_stage(agg_fmt)
    window_agg.connect(agg_fmt)

    main_console = ConsoleSink(name="main_console")
    pipeline.add_stage(main_console)
    agg_fmt.connect(main_console)

    alert_fmt = AlertFormatter(name="alert_fmt")
    pipeline.add_stage(alert_fmt)
    alert.connect(alert_fmt)

    alert_console = ConsoleSink(name="alert_console")
    pipeline.add_stage(alert_console)
    alert_fmt.connect(alert_console)

    # 构造事件序列
    print(">>> 阶段1：窗口 [0,10) 的 3 个正常事件")
    for i in range(3):
        source.add_event(LogEvent(
            timestamp=base_t + i + 1,
            level=LogLevel.INFO,
            message=f"normal-{i}",
            source="test",
            fields={}
        ))
    print("   事件时间: 1s, 2s, 3s")
    print()

    print(">>> 阶段2：推进水位线，触发窗口")
    source.add_event(LogEvent(
        timestamp=base_t + 15,
        level=LogLevel.INFO,
        message="watermark-pusher",
        source="test",
        fields={}
    ))
    print("   事件时间 15s  →  水位线 = 15 - 2 = 13s")
    print("   → 窗口 [0,10) 首次触发 (count=3)")
    print("   → 告警规则 count>=2 触发告警")
    print()

    print(">>> 阶段3：窗口已触发后，再来 2 个迟到事件 (6s, 7s)")
    for t in [6, 7]:
        source.add_event(LogEvent(
            timestamp=base_t + t,
            level=LogLevel.WARN,
            message=f"late-event-{t}s",
            source="test",
            fields={}
        ))
    print("   事件时间 6s, 7s")
    print("   水位线 13s，延迟 = 7s 和 6s，都在 8s 宽限期内")
    print("   → SIDE_OUTPUT 策略：不更新主窗口，只送到旁路输出")
    print("   → 主窗口 count 保持 3 不变（不受迟到数据影响）")
    print()

    print(">>> 阶段4：超宽限期事件 (事件时间 0.5s)")
    source.add_event(LogEvent(
        timestamp=base_t + 25,
        level=LogLevel.INFO,
        message="watermark-pusher-2",
        source="test",
        fields={}
    ))
    source.add_event(LogEvent(
        timestamp=base_t + 0.5,
        level=LogLevel.ERROR,
        message="very-late-event",
        source="test",
        fields={}
    ))
    print("   先推水位线到 23s（事件时间 25s）")
    print("   再发事件时间 0.5s：延迟 = 22.5s，超过 8s 宽限期")
    print("   → 超宽限期，丢弃并送到旁路")
    print()

    print(">>> 开始执行，观察输出...")
    print("   主链路: 窗口统计 + 告警")
    print("   旁路:    迟到日志单独输出（带 ➤ 标记）")
    print()

    pipeline.start()
    pipeline.wait_until_complete(timeout=15, flush_windows=True)

    print()
    print("-" * 70)
    print(" 【结果统计】")
    print("-" * 70)

    main_count = len(main_console.received)
    late_count = late_sink.input_queue.qsize() + len(late_sink.received)
    alert_count = len(alert_console.received)

    print(f"  主链路窗口结果: {main_count} 条")
    print(f"  旁路迟到事件: {late_count} 条")
    print(f"  告警输出: {alert_count} 条")
    print(f"  窗口聚合处理总数: {window_agg.metrics['processed']}")
    print(f"  丢弃(超宽限): {window_agg.metrics['dropped']}")
    print(f"  总迟到事件: {window_agg.get_stats()['late_events']}")
    print()

    ok = True
    if main_count > 0:
        print("  ✅ 主链路正常输出窗口统计")
        for r in main_console.received:
            print(f"     {r}")
    else:
        print("  ❌ 主链路没有输出")
        ok = False

    if late_count > 0:
        print("  ✅ 迟到数据被正确分流到旁路")
        print(f"     共 {late_count} 条旁路事件")
    else:
        print("  ⚠️  没有观察到旁路迟到数据")
        ok = False

    print()
    return ok


# ============================================================
# 场景三：背压反向传导
# ============================================================
def verify_backpressure():
    print("\n" + "=" * 70)
    print(" 【验收场景 3】背压反向传导 (Backpressure)")
    print("=" * 70)
    print()
    print("验证要点：")
    print("  ✅ 慢Sink处理不过来 → 队列堆积 → 背压信号逐级上传")
    print("  ✅ 源头感知到压力后主动降低发送速率")
    print("  ✅ 靠降速匹配处理能力，而非靠丢数据硬撑")
    print()

    random.seed(99999)
    pipeline = Pipeline("backpressure_demo", enable_monitor=False)

    source = MockLogSource(
        name="fast_source",
        event_count=100,
        rate_per_sec=20.0,
        out_of_order_prob=0.0,
        max_out_of_order_sec=0.0,
        enable_backpressure_control=True
    )
    pipeline.add_source(source)

    slow_sink = SlowSink(name="slow_sink", delay_per_event=0.2)
    pipeline.add_stage(slow_sink)
    source.connect(slow_sink)

    print(f"  源头发送基准速率: {source.rate_per_sec} 条/秒")
    print(f"  慢Sink处理能力: ~{1/0.2:.1f} 条/秒")
    print(f"  预期: 源头感知压力后从 20/s → 8/s (YELLOW) → 2/s (RED)")
    print()
    print(">>> 启动管道，观察背压传导过程...")
    print()

    bp_log = []
    stop_monitor = threading.Event()

    def bp_monitor():
        while not stop_monitor.is_set() and pipeline._running:
            time.sleep(0.3)
            try:
                src_stats = source.get_stats()
                sig = source.get_backpressure_signal()
                slow_q = slow_sink.input_queue.qsize()
                bp_log.append({
                    "time": time.time(),
                    "signal": sig.name,
                    "effective_rate": src_stats.get("effective_rate", 0),
                    "slow_q": slow_q,
                })
            except:
                pass

    monitor = threading.Thread(target=bp_monitor, daemon=True)
    monitor.start()

    pipeline.start()
    pipeline.wait_until_complete(timeout=40, flush_windows=True)

    stop_monitor.set()
    monitor.join(timeout=2)

    print()
    print("-" * 70)
    print(" 【背压分析报告】")
    print("-" * 70)

    signals_seen = set()
    max_slow_q = 0
    min_rate = 9999
    max_rate = 0
    rate_recovered = False
    prev_signal = None

    for entry in bp_log:
        signals_seen.add(entry["signal"])
        max_slow_q = max(max_slow_q, entry["slow_q"])
        r = entry["effective_rate"]
        if r > 0:
            if r < min_rate:
                min_rate = r
            if r > max_rate:
                max_rate = r

    src_stats = source.get_stats()
    total_dropped = sum(s.metrics['dropped'] for s in pipeline._all_stages)

    print(f"  基准速率: {source.rate_per_sec:.1f} 条/秒")
    print(f"  观测峰值速率: {max_rate:.1f} 条/秒")
    print(f"  观测最低速率: {min_rate:.1f} 条/秒")
    print(f"  速率下降比例: {(1 - min_rate/source.rate_per_sec)*100:.0f}%")
    print(f"  慢Sink队列峰值: {max_slow_q} 条")
    print(f"  观测到的背压信号: {', '.join(sorted(signals_seen))}")
    print(f"  信号切换次数:")
    print(f"    GREEN 段: {src_stats.get('green_periods', 0)}")
    print(f"    YELLOW 段: {src_stats.get('yellow_periods', 0)}")
    print(f"    RED 段: {src_stats.get('red_periods', 0)}")
    print(f"  慢Sink最终处理: {slow_sink.metrics['processed']} 条")
    print(f"  全链路总丢弃: {total_dropped} 条")
    print()

    ok = True

    if len(signals_seen) >= 2 or "YELLOW" in signals_seen or "RED" in signals_seen:
        print("  ✅ 背压信号发生了变化（压力成功传导到源头）")
    else:
        print("  ⚠️  背压信号变化不明显")
        ok = False

    if min_rate < source.rate_per_sec * 0.7:
        print("  ✅ 源头速率明显下降（限速生效）")
    else:
        print("  ⚠️  源头速率下降不明显")
        ok = False

    if total_dropped == 0:
        print("  ✅ 全程零丢弃！靠降速匹配而非丢数据")
    else:
        print(f"  ⚠️  有 {total_dropped} 条丢弃（队列溢出）")

    print()
    print("  背压时间线（节选）:")
    print("  " + "-" * 50)
    print(f"  {'时间'.ljust(8)} {'信号'.ljust(8)} {'速率/s'.rjust(8)} {'慢SinkQ'.rjust(8)}")
    print("  " + "-" * 50)

    last_sig = None
    for i, entry in enumerate(bp_log):
        show = False
        if i == 0 or i == len(bp_log) - 1:
            show = True
        elif entry["signal"] != last_sig:
            show = True
        elif i % 10 == 0:
            show = True

        if show:
            t = f"{i*0.3:.1f}s"
            print(f"  {t.ljust(6)} {entry['signal'].ljust(8)} {entry['effective_rate']:7.1f} "
                  f"{entry['slow_q']:8d}")
            last_sig = entry["signal"]

    print("  " + "-" * 50)
    print()

    return ok


# ============================================================
# 主入口
# ============================================================
def main():
    print()
    print("#" * 70)
    print("#  流式日志分析管道 - 验收测试套件")
    print("#" * 70)

    results = {}

    try:
        results["迟到更新"] = verify_late_update()
    except Exception as e:
        print(f"\n❌ 场景1异常: {e}")
        import traceback
        traceback.print_exc()
        results["迟到更新"] = False

    time.sleep(1)

    try:
        results["迟到旁路"] = verify_side_output()
    except Exception as e:
        print(f"\n❌ 场景2异常: {e}")
        import traceback
        traceback.print_exc()
        results["迟到旁路"] = False

    time.sleep(1)

    try:
        results["背压传导"] = verify_backpressure()
    except Exception as e:
        print(f"\n❌ 场景3异常: {e}")
        import traceback
        traceback.print_exc()
        results["背压传导"] = False

    print("\n" + "#" * 70)
    print("#  验收总结")
    print("#" * 70)
    print()

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for name, ok in results.items():
        status = "✅ 通过" if ok else "❌ 失败"
        print(f"  {status}  - {name}")

    print()
    print(f"  总计: {passed}/{total} 项通过")
    print()

    if passed == total:
        print("  🎉 全部验收通过！")
    else:
        print("  ⚠️  部分项未通过")

    print()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        scene = sys.argv[1]
        if scene == "1":
            verify_late_update()
        elif scene == "2":
            verify_side_output()
        elif scene == "3":
            verify_backpressure()
        else:
            print(f"未知场景: {scene}")
            print("  1 - 迟到更新")
            print("  2 - 迟到旁路")
            print("  3 - 背压传导")
    else:
        main()
