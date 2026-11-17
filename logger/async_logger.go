// 本文件供业务调用者创建独立日志实例：校验字段、用zap Console编码，再将独立字节放入有界队列。
// 队列容量不足或编码额度占满时立即拒绝；后台逐事件调用Writer写出，关闭排空后每个输出目标刷新一次，失败只更新统计，不修改业务状态。
// Stats返回取得时刻的独立统计副本；Close在有效ctx下封闭准入，按等待期限观察排空、刷新和活动编码是否结束，接收或等待返回不保证持久保存。
// 等待期限不能取消阻塞的Write或Sync，输出目标所有者须中止阻塞、等待完成并最终关闭目标。
// 本实现依赖async_config.go的配置校验与结果定义；原全局Console和同步格式化入口继续保持原行为。
package clogger

import (
	"context"
	"errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"
)

// AsyncLogger 是NewAsyncLogger创建的独立日志实例，使用zap Console编码器并串行调用借用出口。
// 调用者须使用有效实例、结束时Close/Wait并释放出口；日志失败不修改业务状态。
type AsyncLogger struct {
	config     AsyncConfig      // 构造时复制的身份、允许清单和容量。
	outputs    []AsyncOutput    // 构造时固定的借用出口，生命周期由owner负责。
	encoder    zapcore.Encoder  // 空且不变的Console编码器，各调用使用独立临时上下文。
	mu         sync.Mutex       // 保护活动预算和观察数据，出口调用不占锁。
	state      LogStatsSnapshot // mu保护的实时准入和输出统计；Stats另行复制供调用者观察。
	queue      [][]byte         // 仅保留已接管的独立事件字节。
	wake       chan struct{}    // 合并投递和关闭唤醒。
	done       chan struct{}    // 后台和活动编码均结束才关闭。
	doneClosed bool             // 在mu保护下只确认一次完成。
}

var v2InputError = errors.New("受管事件输入不合法")

// NewAsyncLogger 创建受管实例；调用者须在结束时关闭实例并释放自己的出口。
func NewAsyncLogger(config AsyncConfig, outputs []AsyncOutput) (*AsyncLogger, error) {
	checked, copied, err := validateAsyncConfig(config, outputs)
	if err != nil {
		return nil, err
	}
	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{TimeKey: "ts", LevelKey: "level", MessageKey: "msg", LineEnding: "\n", EncodeTime: func(t time.Time, e zapcore.PrimitiveArrayEncoder) { e.AppendString(t.UTC().Format(time.RFC3339)) }, EncodeLevel: zapcore.CapitalLevelEncoder})
	a := &AsyncLogger{config: checked, outputs: copied, encoder: encoder, wake: make(chan struct{}, 1), done: make(chan struct{})}
	for _, output := range copied {
		a.state.Outputs = append(a.state.Outputs, LogOutputStatsSnapshot{Name: output.Name})
	}
	go a.v2Run()
	return a, nil
}

// TryLog 在有效实例上验证等级、消息和成对字段，按非阻塞额度接收事件并更新对应计数。
// 接收不代表写出；拒绝不要求业务回滚或重试。先检查关闭和编码额度，再校验、过滤和检查队列。
func (a *AsyncLogger) TryLog(level, message string, pairs ...any) Result {
	a.mu.Lock()
	if a.state.Closed {
		a.state.ClosedRejected++
		a.mu.Unlock()
		return Result{Status: "closed"}
	}
	if a.state.EncodersActive >= a.config.MaxEncoders {
		a.state.EncodingBusy++
		a.mu.Unlock()
		return Result{Status: "encoding_busy"}
	}
	a.state.EncodersActive++
	a.mu.Unlock()
	defer a.v2ReleaseEncoder()
	owned, rank, err := a.encodeEvent(level, message, pairs)
	if err != nil {
		a.mu.Lock()
		a.state.Invalid++
		a.mu.Unlock()
		return Result{Status: "invalid", Error: err}
	}
	minimum, _ := asyncLevel(a.config.Level)
	if rank < minimum {
		a.mu.Lock()
		a.state.Filtered++
		a.mu.Unlock()
		return Result{Status: "filtered"}
	}
	a.mu.Lock()
	if a.state.Closed {
		a.state.ClosedRejected++
		a.mu.Unlock()
		return Result{Status: "closed"}
	}
	if a.state.PendingEntries >= a.config.MaxEntries || len(owned) > a.config.MaxBytes-a.state.PendingBytes {
		a.state.Full++
		a.mu.Unlock()
		return Result{Status: "full"}
	}
	a.queue = append(a.queue, owned)
	a.state.Accepted++
	a.state.PendingEntries++
	a.state.PendingBytes += len(owned)
	a.mu.Unlock()
	a.v2Wake()
	return Result{Status: "accepted"}
}

// 检查事件并生成自己的Console字节；内部方法不执行Writer，供准入前使用。
func (a *AsyncLogger) encodeEvent(level, message string, pairs []any) ([]byte, int, error) {
	rank, ok := asyncLevel(level)
	if !ok || len(pairs)%2 != 0 || len(pairs)/2 > a.config.MaxPairs || len(message) > a.config.MaxMessageBytes || !utf8.ValidString(message) {
		return nil, 0, v2InputError
	}
	for _, r := range message {
		if r < 32 || r == 127 {
			return nil, 0, v2InputError
		}
	}
	fields := []zapcore.Field{zap.String("service", a.config.Service), zap.String("instance", a.config.Instance), zap.String("version", a.config.Version)}
	seen := make(map[string]bool, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		key, ok := pairs[i].(string)
		if !ok || !validAsyncKey(key, a.config.MaxKeyBytes) || reservedAsyncKey(key) || seen[key] {
			return nil, 0, v2InputError
		}
		seen[key] = true
		kind, ok := a.config.AllowedFields[key]
		if !ok {
			return nil, 0, v2InputError
		}
		field, err := a.v2Scalar(key, kind, pairs[i+1])
		if err != nil {
			return nil, 0, err
		}
		fields = append(fields, field)
	}
	now := time.Now().UTC()
	if now.Year() < 0 || now.Year() > 9999 {
		return nil, 0, v2InputError
	}
	entry := zapcore.Entry{Time: now, Level: zapcore.Level(rank - 1), Message: message}
	exact := 20 + len(entry.Level.CapitalString()) + 3 + len(message) + v2ContextBytes(fields) + 1
	if exact > a.config.MaxEventBytes {
		return nil, 0, v2InputError
	}
	buffer, err := a.encoder.EncodeEntry(entry, fields)
	if err != nil {
		return nil, 0, v2InputError
	}
	defer buffer.Free()
	if buffer.Len() != exact || buffer.Len() > a.config.MaxEventBytes {
		return nil, 0, v2InputError
	}
	owned := make([]byte, buffer.Len())
	copy(owned, buffer.Bytes())
	return owned, rank, nil
}

// 按已批准的zap标量计算Console上下文长度，类型不匹配不走反射编码。
func v2ContextBytes(fields []zapcore.Field) int {
	length := 2
	for index, field := range fields {
		if index > 0 {
			length += 2
		}
		length += len(field.Key) + 4
		switch field.Type {
		case zapcore.StringType:
			length += 2 + v2EscapedBytes(field.String)
		case zapcore.BoolType:
			length += len(strconv.FormatBool(field.Integer != 0))
		case zapcore.Int64Type:
			length += len(strconv.FormatInt(field.Integer, 10))
		case zapcore.Uint64Type:
			length += len(strconv.FormatUint(uint64(field.Integer), 10))
		}
	}
	return length
}

// 校验后的UTF-8字段按zap的JSON转义规则逐字节计长，不截断内容。
func v2EscapedBytes(value string) int {
	length := 0
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\\', '"', '\n', '\r', '\t':
			length += 2
		default:
			if value[i] < 32 {
				length += 6
			} else {
				length++
			}
		}
	}
	return length
}

// 按允许清单生成安全zap标量，不执行任意对象方法。
func (a *AsyncLogger) v2Scalar(key string, kind FieldKind, value any) (zapcore.Field, error) {
	switch v := value.(type) {
	case string:
		if kind == StringField && len(v) <= a.config.MaxStringBytes && utf8.ValidString(v) {
			return zap.String(key, v), nil
		}
	case bool:
		if kind == BoolField {
			return zap.Bool(key, v), nil
		}
	case int:
		if kind == SignedField {
			return zap.Int64(key, int64(v)), nil
		}
	case int64:
		if kind == SignedField {
			return zap.Int64(key, v), nil
		}
	case uint:
		if kind == UnsignedField {
			return zap.Uint64(key, uint64(v)), nil
		}
	case uint64:
		if kind == UnsignedField {
			return zap.Uint64(key, v), nil
		}
	}
	return zapcore.Field{}, v2InputError
}

// Debugw 按debug等级投递成对安全字段。
func (a *AsyncLogger) Debugw(message string, pairs ...any) Result {
	return a.TryLog("debug", message, pairs...)
}

// Infow 按info等级投递成对安全字段。
func (a *AsyncLogger) Infow(message string, pairs ...any) Result {
	return a.TryLog("info", message, pairs...)
}

// Warnw 按warn等级投递成对安全字段。
func (a *AsyncLogger) Warnw(message string, pairs ...any) Result {
	return a.TryLog("warn", message, pairs...)
}

// Errorw 按error等级投递成对安全字段，不改变业务或进程状态。
func (a *AsyncLogger) Errorw(message string, pairs ...any) Result {
	return a.TryLog("error", message, pairs...)
}

// Stats 持锁取得当前队列、接收、拒绝和失败统计，并复制Outputs。
// 调用者修改结果不影响实例，后台变化不更新已返回副本；此查询不使用输出队列。
func (a *AsyncLogger) Stats() LogStatsSnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	result := a.state
	result.Outputs = append([]LogOutputStatsSnapshot(nil), a.state.Outputs...)
	return result
}

// Close 在ctx非nil时先封闭准入，再按ctx期限等待后台和活动编码结束。
// nil只返回invalid_context且不封闭准入；超时不取消Write或Sync，调用者仍须中止出口、Wait并最终释放。
func (a *AsyncLogger) Close(ctx context.Context) CloseReport {
	if ctx == nil {
		return a.v2Report("invalid_context")
	}
	a.mu.Lock()
	a.state.Closed = true
	a.mu.Unlock()
	a.v2Wake()
	return a.Wait(ctx)
}

// Wait 按ctx等待已有实例完成，不封闭准入、不启动关闭；应先调用Close。
// nil、取消或期限到达只结束本次等待，不中止出口；finished也不表示输出成功。
func (a *AsyncLogger) Wait(ctx context.Context) CloseReport {
	if ctx == nil {
		return a.v2Report("invalid_context")
	}
	if err := ctx.Err(); err != nil {
		return a.v2Report(v2WaitStatus(err))
	}
	status := "finished"
	select {
	case <-a.done:
	case <-ctx.Done():
		status = v2WaitStatus(ctx.Err())
	}
	return a.v2Report(status)
}

// 仅判断标准等待类别，不执行错误文字方法。
func v2WaitStatus(err error) string {
	if err == context.DeadlineExceeded {
		return "deadline"
	}
	return "canceled"
}

// 快照把排空、任务结束与等待结果分开，不以等待返回表示输出成功。
func (a *AsyncLogger) v2Report(status string) CloseReport {
	s := a.Stats()
	return CloseReport{LogStatsSnapshot: s, Drained: s.PendingEntries == 0, WaitStatus: status}
}

// 归还前台额度，并与已经结束的后台共同确认完成。
func (a *AsyncLogger) v2ReleaseEncoder() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.state.EncodersActive--
	a.v2DoneLocked()
}

// 调用者持mu；完成信号不早于后台退出及全部前台额度归还。
func (a *AsyncLogger) v2DoneLocked() {
	if a.state.WorkerExited && a.state.EncodersActive == 0 && !a.doneClosed {
		a.doneClosed = true
		close(a.done)
	}
}

// 唤醒可以合并，因为后台始终重新读取受锁状态。
func (a *AsyncLogger) v2Wake() {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

// 先取得独立队列项再解锁写出，活动预算直到各出口尝试结束才归还。
func (a *AsyncLogger) v2Run() {
	for {
		a.mu.Lock()
		if len(a.queue) == 0 {
			closed := a.state.Closed
			a.mu.Unlock()
			if closed {
				break
			}
			<-a.wake
			continue
		}
		item := a.queue[0]
		a.queue[0] = nil
		a.queue = a.queue[1:]
		a.mu.Unlock()
		success := a.v2WriteOnce(item)
		a.mu.Lock()
		a.state.Completed++
		if success {
			a.state.Succeeded++
		}
		a.state.PendingEntries--
		a.state.PendingBytes -= len(item)
		a.mu.Unlock()
	}
	flushed := true
	for index, output := range a.outputs {
		if output.Writer.Sync() != nil {
			flushed = false
			a.mu.Lock()
			a.state.Outputs[index].SyncFailed++
			a.state.Outputs[index].LastFailure = "sync_failed"
			a.mu.Unlock()
		}
	}
	a.mu.Lock()
	a.state.FlushCompleted = true
	a.state.FlushSucceeded = flushed
	a.state.WorkerExited = true
	a.v2DoneLocked()
	a.mu.Unlock()
}

// 每事件每出口仅一次Write；故障分类不存储原错误，首出口返回后仍尝试下一出口。
func (a *AsyncLogger) v2WriteOnce(item []byte) bool {
	allSucceeded := true
	for index, output := range a.outputs {
		n, err := output.Writer.Write(item)
		short := n >= 0 && n < len(item)
		category := ""
		switch {
		case n < 0 || n > len(item):
			category = "invalid_write_count"
		case err != nil:
			category = "write_failed"
		case short:
			category = "short_write"
		}
		a.mu.Lock()
		counter := &a.state.Outputs[index]
		if category == "" {
			counter.Succeeded++
		} else {
			allSucceeded = false
			counter.Failed++
			counter.LastFailure = category
		}
		if short {
			counter.ShortWrites++
		}
		a.mu.Unlock()
	}
	return allSucceeded
}
