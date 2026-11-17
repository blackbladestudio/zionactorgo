// 本文件使用可释放出口和同步屏障验证后台交付、容量与关闭，不连接真实网络。
// 每个阻塞夹具都由测试释放并等待实例结束，避免故障测试遗留后台任务。
package clogger

import (
	"context"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
	"strings"
	"sync"
	"testing"
	"time"
)

// 故障错误对象禁止转成文字，保证后台仅记录固定类别。
type opaqueAsyncError struct{}

// Error 若被调用即暴露契约违反，测试直接失败而不返回秘密内容。
func (opaqueAsyncError) Error() string { panic("不能调用Writer错误的Error") }

// 故障出口第一次返回指定故障，后续完整写；次数由测试关停后核对。
type faultOutput struct {
	memoryOutput
	mode      string // 第一次Write的故障类别。
	calls     int    // 仅后台修改，关闭完成后读取。
	syncCalls int    // 仅后台修改，重复关闭不能增加。
	syncError bool   // 是否返回不允许文字化的刷新错误。
}

// 顺序投递按接受次序写出，调用者之后修改参数不得改变已接收字节。
func TestAsyncFIFOAndOwnedBytes(t *testing.T) {
	w := newGateOutput()
	a, err := NewAsyncLogger(validAsyncConfig(), testOutputs(w))
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			close(w.release)
		}
		a.Close(context.Background())
	}()
	if a.Infow("首项", "event", "first").Status != "accepted" {
		t.Fatal("合法接收前提失败")
	}
	awaitAsync(t, w.entered, "实际Write前提失败")
	pairs := []any{"event", "second"}
	if a.Infow("第二项", pairs...).Status != "accepted" {
		t.Fatal("合法第二项前提失败")
	}
	pairs[1] = "changed"
	if a.Infow("第三项", "event", "third").Status != "accepted" {
		t.Fatal("合法第三项前提失败")
	}
	close(w.release)
	released = true
	r := a.Close(context.Background())
	w.mu.Lock()
	text := string(w.data)
	w.mu.Unlock()
	first, second, third := strings.Index(text, `"event": "first"`), strings.Index(text, `"event": "second"`), strings.Index(text, `"event": "third"`)
	if first < 0 || second <= first || third <= second || strings.Contains(text, "changed") || r.Succeeded != 3 {
		t.Fatalf("FIFO或独立字节不符: %q", text)
	}
}

// 并发投递和重复关闭后等待所有调用返回，核对接收与拒绝统计不遗漏。
func TestAsyncConcurrentCloseAndAccounting(t *testing.T) {
	c := validAsyncConfig()
	c.MaxEntries = 32
	c.MaxEncoders = 8
	a, err := NewAsyncLogger(c, testOutputs(&memoryOutput{}))
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan Result, 128)
	var calls sync.WaitGroup
	for i := 0; i < 128; i++ {
		calls.Add(1)
		go func() { defer calls.Done(); <-start; results <- a.Infow("并发申请") }()
	}
	close(start)
	var closers sync.WaitGroup
	for i := 0; i < 4; i++ {
		closers.Add(1)
		go func() { defer closers.Done(); a.Close(context.Background()) }()
	}
	calls.Wait()
	closers.Wait()
	close(results)
	counts := map[string]uint64{}
	for r := range results {
		counts[r.Status]++
	}
	s := a.Stats()
	if s.Accepted != counts["accepted"] || s.Full != counts["full"] || s.EncodingBusy != counts["encoding_busy"] || s.ClosedRejected != counts["closed"] || s.Accepted+s.Full+s.EncodingBusy+s.ClosedRejected != 128 || s.PendingEntries != 0 || s.EncodersActive != 0 || !s.WorkerExited {
		t.Fatalf("并发关闭记账遗漏: %+v counts=%v", s, counts)
	}
}

// Write 每次只返回一个结果，组件不能补写当前残行。
func (w *faultOutput) Write(p []byte) (int, error) {
	w.calls++
	if w.calls > 1 {
		return len(p), nil
	}
	switch w.mode {
	case "zero":
		return 0, nil
	case "short":
		return len(p) - 1, nil
	case "partial_error":
		return 1, opaqueAsyncError{}
	case "error":
		return 0, opaqueAsyncError{}
	case "negative":
		return -1, nil
	case "excess":
		return len(p) + 1, nil
	}
	return len(p), nil
}

// Sync 记录刷新次数，并用不允许文字化的错误检查失败统计。
func (w *faultOutput) Sync() error {
	w.syncCalls++
	if w.syncError {
		return opaqueAsyncError{}
	}
	return nil
}

// 在后台已能实际Write之后验证各故障类别、一次尝试及第二出口继续。
func TestAsyncWriteFailuresAndContinue(t *testing.T) {
	for _, mode := range []string{"zero", "short", "partial_error", "error", "negative", "excess"} {
		t.Run(mode, func(t *testing.T) {
			bad := &faultOutput{mode: mode}
			good := &faultOutput{}
			outputs := []AsyncOutput{{Name: "bad", Owner: "测试", Writer: bad}, {Name: "good", Owner: "测试", Writer: good}}
			a, err := NewAsyncLogger(validAsyncConfig(), outputs)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close(context.Background())
			for i := 0; i < 2; i++ {
				if result := a.Infow("申请事件"); result.Status != "accepted" {
					t.Fatal("合法接收前提失败")
				}
			}
			r := a.Close(context.Background())
			if bad.calls != 2 || good.calls != 2 {
				t.Fatal("每事件每出口必须一次尝试，首出口失败后不能跳过其他出口")
			}
			short := uint64(1)
			category := "short_write"
			if mode == "partial_error" || mode == "error" {
				category = "write_failed"
			}
			if mode == "negative" || mode == "excess" {
				short = 0
				category = "invalid_write_count"
			}
			if r.Completed != 2 || r.Succeeded != 1 || r.Outputs[0].Failed != 1 || r.Outputs[0].Succeeded != 1 || r.Outputs[0].ShortWrites != short || r.Outputs[0].LastFailure != category || r.Outputs[1].Succeeded != 2 || r.PendingBytes != 0 {
				t.Fatalf("故障或部分成功统计不符: %+v", r)
			}
		})
	}
}

// Sync错误须与已返回的刷新过程分开，重复Close不能第二次刷新。
func TestAsyncFlushFailureAndRepeatedClose(t *testing.T) {
	w := &faultOutput{syncError: true}
	a, err := NewAsyncLogger(validAsyncConfig(), testOutputs(w))
	if err != nil {
		t.Fatal(err)
	}
	r := a.Close(context.Background())
	repeat := a.Close(context.Background())
	if !r.FlushCompleted || r.FlushSucceeded || !r.WorkerExited || r.Outputs[0].SyncFailed != 1 || r.Outputs[0].LastFailure != "sync_failed" || w.syncCalls != 1 || repeat.WaitStatus != "finished" {
		t.Fatalf("刷新失败不应谎报成功或重复刷新: %+v", r)
	}
}

// nil context不能关闭实例或panic；已取消的Close仍封准入并准确报告等待原因。
func TestAsyncContextSemantics(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		a, err := NewAsyncLogger(validAsyncConfig(), testOutputs(&memoryOutput{}))
		if err != nil {
			t.Fatal(err)
		}
		defer a.Close(context.Background())
		func() {
			defer func() {
				if recover() != nil {
					t.Fatal("nil context应返回invalid_context而非panic")
				}
			}()
			r := a.Close(nil)
			if r.WaitStatus != "invalid_context" || r.Closed {
				t.Fatal("nil context不得封准入")
			}
			if r := a.Wait(nil); r.WaitStatus != "invalid_context" {
				t.Fatal("Wait nil应返回invalid_context")
			}
		}()
	})
	t.Run("canceled", func(t *testing.T) {
		a, err := NewAsyncLogger(validAsyncConfig(), testOutputs(&memoryOutput{}))
		if err != nil {
			t.Fatal(err)
		}
		defer a.Close(context.Background())
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r := a.Close(ctx)
		if !r.Closed || r.WaitStatus != "canceled" {
			t.Fatalf("已取消Close仍封准入且报告canceled: %+v", r)
		}
	})
}

// 刷新屏障单独证明零剩余不等于刷新完成或后台退出，owner必须释放后等待。
type syncGateOutput struct {
	memoryOutput
	entered chan struct{} // 确认已经进入Sync。
	release chan struct{} // owner关闭后解除刷新阻塞。
	once    sync.Once     // 防止重复关闭通知信号。
}

// Sync 在刷新阶段阻塞，由测试负责人明确中止，不依赖context取消。
func (w *syncGateOutput) Sync() error {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return nil
}

// Write和Sync阻塞时Close按预算返回，最后释放屏障并Wait确认真实退出。
func TestAsyncBlockedWriteAndSync(t *testing.T) {
	for _, phase := range []string{"write", "sync"} {
		t.Run(phase, func(t *testing.T) {
			var writer AsyncWriter
			var entered, release chan struct{}
			if phase == "write" {
				w := newGateOutput()
				writer = w
				entered = w.entered
				release = w.release
			} else {
				w := &syncGateOutput{entered: make(chan struct{}), release: make(chan struct{})}
				writer = w
				entered = w.entered
				release = w.release
			}
			a, err := NewAsyncLogger(validAsyncConfig(), testOutputs(writer))
			if err != nil {
				t.Fatal(err)
			}
			closeReturned := make(chan struct{})
			closeStarted := false
			defer func() {
				close(release)
				a.Close(context.Background())
				if closeStarted {
					<-closeReturned
				}
			}()
			if phase == "write" {
				if a.Infow("受控事件").Status != "accepted" {
					t.Fatal("合法接收前提失败")
				}
				awaitAsync(t, entered, "实际Write阻塞前提失败")
			} else {
				closeStarted = true
				go func() { a.Close(context.Background()); close(closeReturned) }()
				awaitAsync(t, entered, "实际Sync阻塞前提失败")
			}
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			defer cancel()
			r := a.Close(ctx)
			if r.WaitStatus != "deadline" || r.WorkerExited || r.FlushCompleted {
				t.Fatalf("预算不能取消底层或伪造完成: %+v", r)
			}
			if phase == "write" && (r.PendingEntries != 1 || r.PendingBytes == 0) {
				t.Fatal("Write中项必须报告剩余")
			}
			if phase == "sync" && (!r.Drained || r.PendingEntries != 0) {
				t.Fatal("Sync阻塞时排空与刷新分开")
			}
		})
	}
}

// 屏障出口在第一次写入时通知测试，只有测试释放后才继续。
type gateOutput struct {
	memoryOutput               // 保存实际内容，供后续核对。
	entered      chan struct{} // 确认后台确实已进入Write。
	release      chan struct{} // 测试负责关闭，解除所有Write阻塞。
	once         sync.Once     // 首次进入通知只发送一次。
}

// 在编码器入口设置屏障，仅用于证明公开投递的并发额度。
type gateEncoder struct {
	zapcore.Encoder               // 原不变Console编码器，释放后执行真实编码。
	entered         chan struct{} // 确认编码入口已到达。
	release         chan struct{} // 测试释放所有编码调用。
	once            sync.Once     // 首次进入通知只发送一次。
}

// EncodeEntry 确认前台真正进入编码阶段，测试负责释放。
func (e *gateEncoder) EncodeEntry(entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	e.once.Do(func() { close(e.entered) })
	<-e.release
	return e.Encoder.EncodeEntry(entry, fields)
}

// 在真实Write已阻塞的前提下检查条数和字节拒绝，避免把无后台交付当容量红灯。
func TestAsyncCapacityIncludesWriting(t *testing.T) {
	for _, kind := range []string{"entries", "bytes"} {
		t.Run(kind, func(t *testing.T) {
			c := validAsyncConfig()
			if kind == "entries" {
				c.MaxEntries = 1
			} else {
				c.MaxEventBytes = 200
				c.MaxBytes = 200
			}
			w := newGateOutput()
			a, err := NewAsyncLogger(c, testOutputs(w))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				close(w.release)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				a.Close(ctx)
			}()
			if r := a.Infow("申请已登记", "event", "registered"); r.Status != "accepted" {
				t.Fatal("合法接收前提失败")
			}
			awaitAsync(t, w.entered, "后台Write前提失败")
			before := a.Stats()
			if before.PendingEntries != 1 || before.PendingBytes <= 0 {
				t.Fatal("写中事件仍应占预算")
			}
			if r := a.Infow("申请已登记", "event", "registered"); r.Status != "full" {
				t.Fatalf("写中占用必须触发%s预算拒绝，得到%s", kind, r.Status)
			}
			after := a.Stats()
			if after.PendingEntries != 1 || after.PendingBytes != before.PendingBytes || after.Full != 1 {
				t.Fatal("拒绝不得改变活动预算，计数应独立可见")
			}
		})
	}
}

// 多生产者竞争最后两个名额，受控Write保持活动预算不被后台提前释放。
func TestAsyncConcurrentLastSlots(t *testing.T) {
	c := validAsyncConfig()
	c.MaxEntries = 3
	c.MaxEncoders = 32
	w := newGateOutput()
	a, err := NewAsyncLogger(c, testOutputs(w))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { close(w.release); a.Close(context.Background()) }()
	if r := a.Infow("首项"); r.Status != "accepted" {
		t.Fatal("合法接收前提失败")
	}
	awaitAsync(t, w.entered, "后台Write前提失败")
	results := make(chan Result, 16)
	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func() { defer group.Done(); results <- a.Infow("并发申请") }()
	}
	group.Wait()
	close(results)
	accepted, full := 0, 0
	for r := range results {
		switch r.Status {
		case "accepted":
			accepted++
		case "full":
			full++
		default:
			t.Fatalf("非预期拒绝%s", r.Status)
		}
	}
	if accepted != 2 || full != 14 || a.Stats().PendingEntries != 3 {
		t.Fatalf("末名额须原子准入: accepted=%d full=%d pending=%d", accepted, full, a.Stats().PendingEntries)
	}
}

// 第一调用真实编码阻塞后，第二调用必须不等待编码器，并累计编码忙拒绝。
func TestAsyncEncodingQuota(t *testing.T) {
	c := validAsyncConfig()
	c.MaxEncoders = 1
	a, err := NewAsyncLogger(c, testOutputs(&memoryOutput{}))
	if err != nil {
		t.Fatal(err)
	}
	e := &gateEncoder{Encoder: a.encoder, entered: make(chan struct{}), release: make(chan struct{})}
	a.encoder = e
	first := make(chan Result, 1)
	second := make(chan Result, 1)
	secondFinished := false
	defer func() {
		close(e.release)
		<-first
		if !secondFinished {
			<-second
		}
		a.Close(context.Background())
	}()
	go func() { first <- a.Infow("首项") }()
	awaitAsync(t, e.entered, "首调用编码前提失败")
	go func() { second <- a.Infow("第二项") }()
	select {
	case result := <-second:
		secondFinished = true
		if result.Status != "encoding_busy" {
			t.Fatal("编码额度满应立即拒绝")
		}
	case <-time.After(time.Second):
		t.Fatal("第二调用不得等待阻塞的编码器")
	}
	if s := a.Stats(); s.EncodersActive != 1 || s.EncodingBusy != 1 {
		t.Fatal("编码任务和拒绝必须独立可见")
	}
}

// Close不能把编码中调用当作已经返回；释放后该调用只能因closed拒绝。
func TestAsyncCloseWaitsForEncoding(t *testing.T) {
	a, err := NewAsyncLogger(validAsyncConfig(), testOutputs(&memoryOutput{}))
	if err != nil {
		t.Fatal(err)
	}
	e := &gateEncoder{Encoder: a.encoder, entered: make(chan struct{}), release: make(chan struct{})}
	a.encoder = e
	result := make(chan Result, 1)
	defer func() {
		close(e.release)
		r := <-result
		final := a.Close(context.Background())
		if r.Status != "closed" || final.EncodersActive != 0 || !final.WorkerExited {
			t.Error("释放编码后必须拒绝并完成清理")
		}
	}()
	go func() { result <- a.Infow("编码中") }()
	awaitAsync(t, e.entered, "真实编码前提失败")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report := a.Close(ctx)
	if report.EncodersActive != 1 || report.WaitStatus == "finished" {
		t.Fatalf("关闭须报告仍在编码的调用: %+v", report)
	}
}

// 创建可释放夹具；不创建线程或其他需要中止的资源。
func newGateOutput() *gateOutput {
	return &gateOutput{entered: make(chan struct{}), release: make(chan struct{})}
}

// Write 先确认执行到出口，再等待测试明确释放，不用sleep猜后台时序。
func (w *gateOutput) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return w.memoryOutput.Write(p)
}

// 等待实际屏障，超时只用于识别缺失的目标动作，不当作完成确认。
func awaitAsync(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal(description)
	}
}

// 验证accepted真正交给后台出口，并在释放后由Close确认任务结束。
func TestAsyncDispatchAndBasicClose(t *testing.T) {
	w := newGateOutput()
	a, err := NewAsyncLogger(validAsyncConfig(), testOutputs(w))
	if err != nil {
		t.Fatal("合法构造前提失败")
	}
	defer func() {
		close(w.release)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		a.Close(ctx)
	}()
	if r := a.Infow("申请已登记", "event", "registered"); r.Status != "accepted" {
		t.Fatal("合法接收前提失败")
	}
	awaitAsync(t, w.entered, "accepted必须实际启动后台Write")
}

// 验证无待写事件时Close封准入、刷新并等待后台结束，重复调用不重新开始流程。
func TestAsyncEmptyCloseAndSeal(t *testing.T) {
	a, err := NewAsyncLogger(validAsyncConfig(), testOutputs(&memoryOutput{}))
	if err != nil {
		t.Fatal("合法构造前提失败")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r := a.Close(ctx)
	if !r.Closed || !r.Drained || !r.WorkerExited || !r.FlushCompleted || !r.FlushSucceeded || r.WaitStatus != "finished" {
		t.Fatalf("空关闭必须真实完成: %+v", r)
	}
	if result := a.Infow("关闭后"); result.Status != "closed" {
		t.Fatal("Close后不得再接受")
	}
	if repeat := a.Close(ctx); repeat.WaitStatus != "finished" {
		t.Fatal("重复Close应观察同一完成流程")
	}
}
