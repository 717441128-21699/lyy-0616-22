# 流式日志分析管道 - 架构设计与实现说明

## 一、整体架构

```
┌─────────────┐    ┌──────────┐    ┌──────────┐    ┌───────────────┐    ┌─────────┐
│  LogSource  │───▶│  Parse   │───▶│  Filter  │───▶│ WindowAggregation│──▶│  Alert  │
│ (多源输入)   │    │ (解析)    │    │ (过滤)    │    │  (窗口聚合)     │    │ (告警)  │
└─────────────┘    └──────────┘    └──────────┘    └───────────────┘    └─────────┘
       │                                                                      │
       │                              背压信号反向传导                            │
       │◀─────────────────────────────────────────────────────────────────────┘
```

模块文件：
- [core.py](file:///d:/trae-bz/TraeProjects/22/core.py) - 核心数据结构和基类
- [stages.py](file:///d:/trae-bz/TraeProjects/22/stages.py) - 日志源、解析、过滤等基础阶段
- [windowing.py](file:///d:/trae-bz/TraeProjects/22/windowing.py) - 窗口聚合引擎（核心）
- [alerting.py](file:///d:/trae-bz/TraeProjects/22/alerting.py) - 告警阶段
- [pipeline.py](file:///d:/trae-bz/TraeProjects/22/pipeline.py) - 管道调度器和配置构建
- [main.py](file:///d:/trae-bz/TraeProjects/22/main.py) - 7个完整演示用例

---

## 二、重点1：乱序日志的窗口聚合 - 水位线(Watermark)机制

### 2.1 核心问题
分布式环境下日志到达顺序与事件发生时间(event time)不一致，需要确定"何时可以认为某个时间窗口内的所有日志都已到达，可以关闭窗口触发计算"。

### 2.2 水位线判定算法

实现在 [windowing.py](file:///d:/trae-bz/TraeProjects/22/windowing.py) 的 `_update_watermark()` 方法：

```python
def _update_watermark(self, event_time: float):
    if event_time > self._watermark.max_event_time:
        self._watermark.max_event_time = event_time
        new_wm = event_time - self.out_of_orderness
        if new_wm > self._watermark.current_watermark:
            self._watermark.current_watermark = new_wm
```

**原理：**
- 维护系统观察到的最大事件时间 `max_event_time`
- 水位线 = `max_event_time - out_of_orderness`（乱序容忍度）
- 水位线只递增不回退，单调前进
- 当 `watermark >= window.end` 时，判定窗口可以触发计算

### 2.3 窗口关闭判定

```python
def _is_window_ready(self, window: Window) -> bool:
    watermark = self._get_watermark()
    return watermark >= window.end
```

**语义：** "水位线已经超过窗口结束时间，意味着乱序容忍期内的所有事件应该都到了，可以安全计算"。

---

## 三、重点2：迟到日志的三种处理策略

迟到定义：`event.event_time() < (watermark - allowed_lateness)`

实现在 [windowing.py](file:///d:/trae-bz/TraeProjects/22/windowing.py#L274-L317) 的 `_handle_late_event()`。

### 3.1 DISCARD - 直接丢弃
```python
if self.late_strategy == LateEventStrategy.DISCARD:
    self.metrics["dropped"] += 1
    return None
```
适用场景：对最终一致性要求不高，追求性能。

### 3.2 UPDATE - 更新并重发结果
```python
if self.late_strategy == LateEventStrategy.UPDATE:
    fired = self._try_fire_window(win, key, is_late_update=True)
```
- 将迟到数据纳入对应窗口状态
- 重新计算并下发标记为 `is_late_update=True` 的 `AggregationResult`
- 下游需要支持结果修正（覆盖之前发过的结果）

适用场景：需要数据最终准确，下游支持幂等更新。

### 3.3 SIDE_OUTPUT - 侧输出分流
```python
if self.side_output and self.late_strategy == LateEventStrategy.SIDE_OUTPUT:
    self.side_output.input_queue.put(event, timeout=0.01)
```
- 将迟到事件发送到独立的旁路输出
- 主流程不受影响，迟到数据可单独审计或二次处理

适用场景：需要审计所有数据，不能丢弃也不想干扰主流。

---

## 四、重点3：背压反向传导机制

### 4.1 整体设计

每个阶段都有**有界队列**（`queue.Queue(maxsize=1000)`），当下游处理慢导致队列满时：

```
慢Sink队列满 ──▶ 上游emit抛Full异常 ──▶ 上游标记RED/YELLOW ──▶ 更上游检测到BP信号
                                                                      │
                                                                      ▼
                                                              Source降低发送速率
```

### 4.2 信号分级 - BackPressureSignal
在 [core.py](file:///d:/trae-bz/TraeProjects/22/core.py#L83-L95) 定义：
- **GREEN** (0): 正常，无压力
- **YELLOW** (1): 队列达到 100，轻度减速
- **RED** (2): 队列达到 500，重度减速

### 4.3 触发机制 - Stage.emit()
[core.py](file:///d:/trae-bz/TraeProjects/22/core.py#L120-L143):
```python
def emit(self, event: Any):
    for next_stage in self.next_stages:
        try:
            next_stage.input_queue.put(event, timeout=0.1)
        except queue.Full:
            self.metrics["dropped"] += 1
            self._signal_backpressure(next_stage)  # 设置BP信号
```

### 4.4 源头响应 - Source._should_slow_down()
[core.py](file:///d:/trae-bz/TraeProjects/22/core.py#L201-L211):
```python
def _should_slow_down(self) -> bool:
    signal = self.get_backpressure_signal()
    return signal in (BackPressureSignal.YELLOW, BackPressureSignal.RED)

def _get_sleep_time(self) -> float:
    signal = self.get_backpressure_signal()
    if signal == BackPressureSignal.RED:
        return 0.5      # 重度减速：每事件等500ms
    elif signal == BackPressureSignal.YELLOW:
        return 0.1      # 轻度减速：每事件等100ms
    return 0.0
```

MockLogSource在 [stages.py](file:///d:/trae-bz/TraeProjects/22/stages.py#L38-L39) 中主动检测：
```python
while self._should_slow_down() and self._running:
    time.sleep(self._get_sleep_time())
```

### 4.5 信号传导
BP信号是**逐级向上**传递的：每个Stage通过 `get_backpressure_signal()` 查询下游的最严重信号，上游的Source最终据此决定自己的发送速率。

---

## 五、重点4：窗口状态清理机制

### 5.1 为什么需要清理
如果不清理，历史窗口的状态会无限累积，导致内存OOM。

### 5.2 清理策略 - _maybe_cleanup()
[windowing.py](file:///d:/trae-bz/TraeProjects/22/windowing.py#L360-L386):

```python
def _maybe_cleanup(self):
    # 1. 周期性触发（默认每60秒检查一次）
    if now - self._last_cleanup < self.cleanup_interval:
        return

    # 2. 判定窗口可删除的两个条件：
    for state_key, meta in self._window_metadata.items():
        window_end = w_end + self.allowed_lateness
        if wm >= window_end:                    # a. 水位线超过窗口结束+宽容期
            idle_time = now - meta["last_updated"]
            if idle_time > self.cleanup_interval:  # b. 窗口已长时间未更新
                keys_to_remove.append(state_key)

    # 3. 原子删除状态和元数据
    for k in keys_to_remove:
        self._window_states.pop(k, None)
        self._window_metadata.pop(k, None)
```

### 5.3 状态存储结构
- `_window_states: Dict[(key, start, end), state]` - 实际聚合状态
- `_window_metadata: OrderedDict[(key, start, end), meta]` - 追踪创建/更新时间、是否已触发

两个字典同时维护、同时删除，使用 OrderedDict 可以按插入顺序遍历，方便优先清理最老的窗口。

---

## 六、支持的窗口类型

| 类型 | 说明 | 实现 |
|------|------|------|
| TUMBLING | 滚动窗口，不重叠 | [TumblingWindowAssigner](file:///d:/trae-bz/TraeProjects/22/windowing.py#L26-L33) |
| SLIDING | 滑动窗口，可配置重叠 | [SlidingWindowAssigner](file:///d:/trae-bz/TraeProjects/22/windowing.py#L36-L49) |
| SESSION | 会话窗口，基于间隙动态合并 | [SessionWindowAssigner](file:///d:/trae-bz/TraeProjects/22/windowing.py#L52-L57) + [_merge_session_windows](file:///d:/trae-bz/TraeProjects/22/windowing.py#L198-L225) |

---

## 七、运行演示

```bash
# 运行全部7个演示
python main.py

# 运行单个演示（1-7）
python main.py 1   # 基础管道
python main.py 2   # 迟到UPDATE策略
python main.py 3   # 迟到SIDE_OUTPUT策略
python main.py 4   # 背压机制演示
python main.py 5   # 滑动窗口
python main.py 6   # 按source分组聚合
python main.py 7   # 自定义管道组合
```

演示输出每5秒打印一次各阶段的统计：
- processed: 已处理事件数
- dropped: 因背压丢弃数
- BP: 背压信号 (GREEN/YELLOW/RED)
- qsize: 当前输入队列长度
- watermark/active_windows/late_events: 窗口聚合专用指标
