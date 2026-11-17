//go:build linux

// 本文件在Linux固定负载中观察日志投递耗时、CPU和内存，包含采样自身的成本。
// 测试容量不是生产推荐；共享zap缓冲池和Go堆仅记录观测峰值，不宣称硬内存上限。
package clogger

import (
	"context"
	"encoding/json"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"
)

// 实测出口不保留内容，分别模拟正常、固定慢写和持续返回错误。
type measureOutput struct {
	mode string /* 固定实验类别，调用期间不修改。 */
}

// Write 的毫秒等待只用于故障负载测量，不用它证明并发测试的执行顺序。
func (w *measureOutput) Write(p []byte) (int, error) {
	if w.mode == "slow" {
		time.Sleep(time.Millisecond)
	}
	if w.mode == "error" {
		return 0, opaqueAsyncError{}
	}
	return len(p), nil
}

// Sync 明确该内存实验无需刷新，不声明持久化。
func (w *measureOutput) Sync() error { return nil }

// 用进程累计用户与内核CPU秒数观察固定实验开销，不根据墙钟推测CPU。
func measureCPU() float64 {
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) != nil {
		return -1
	}
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}

// 返回已排序采样中的耗时分位值，单位纳秒。
func measurePercentile(samples []int64, percent int) int64 {
	if len(samples) == 0 {
		return 0
	}
	index := (len(samples) - 1) * percent / 100
	return samples[index]
}

// 使用四生产者各256条固定字段，核对故障统计并记录观测值；最终释放所有后台任务。
func TestAsyncCapacityMeasurement(t *testing.T) {
	for _, mode := range []string{"normal", "slow", "error", "blocked"} {
		t.Run(mode, func(t *testing.T) {
			c := validAsyncConfig()
			c.MaxEntries = 32
			c.MaxBytes = 65536
			c.MaxEncoders = 4
			var writer AsyncWriter = &measureOutput{mode: mode}
			var gate *gateOutput
			if mode == "blocked" {
				gate = newGateOutput()
				writer = gate
			}
			a, err := NewAsyncLogger(c, testOutputs(writer))
			if err != nil {
				t.Fatal(err)
			}
			if gate != nil {
				if a.Infow("受控首项", "event", "registered").Status != "accepted" {
					t.Fatal("阻塞实验前提失败")
				}
				awaitAsync(t, gate.entered, "受控Write未进入")
			}
			released := false
			defer func() {
				if gate != nil && !released {
					close(gate.release)
				}
				a.Close(context.Background())
			}()
			encoded, _, err := a.encodeEvent("info", "申请已登记", []any{"event", "registered", "count", int64(3)})
			if err != nil {
				t.Fatal(err)
			}
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			cpuBefore := measureCPU()
			started := time.Now()
			var mu sync.Mutex
			var group sync.WaitGroup
			samples := make([]int64, 0, 1024)
			peakEntries, peakBytes, peakEncoders := 0, 0, 0
			peakAlloc := before.Alloc
			for producer := 0; producer < 4; producer++ {
				group.Add(1)
				go func() {
					defer group.Done()
					for i := 0; i < 256; i++ {
						callStart := time.Now()
						a.Infow("申请已登记", "event", "registered", "count", int64(3))
						elapsed := time.Since(callStart).Nanoseconds()
						s := a.Stats()
						mu.Lock()
						samples = append(samples, elapsed)
						if s.PendingEntries > peakEntries {
							peakEntries = s.PendingEntries
						}
						if s.PendingBytes > peakBytes {
							peakBytes = s.PendingBytes
						}
						if s.EncodersActive > peakEncoders {
							peakEncoders = s.EncodersActive
						}
						mu.Unlock()
						if i%64 == 0 {
							var observed runtime.MemStats
							runtime.ReadMemStats(&observed)
							mu.Lock()
							if observed.Alloc > peakAlloc {
								peakAlloc = observed.Alloc
							}
							mu.Unlock()
						}
					}
				}()
			}
			group.Wait()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			report := a.Close(ctx)
			cancel()
			pendingAtBudget := report.PendingEntries
			bytesAtBudget := report.PendingBytes
			if gate != nil {
				if report.WaitStatus != "deadline" || report.WorkerExited || pendingAtBudget == 0 {
					t.Fatal("阻塞实验应真实报告未完成")
				}
				close(gate.release)
				released = true
			}
			final := a.Wait(context.Background())
			runtime.ReadMemStats(&after)
			if after.Alloc > peakAlloc {
				peakAlloc = after.Alloc
			}
			cpu := measureCPU() - cpuBefore
			duration := time.Since(started)
			if final.WaitStatus != "finished" || !final.WorkerExited || final.PendingEntries != 0 || final.PendingBytes != 0 || final.EncodersActive != 0 {
				t.Fatal("实测结束必须清理所有任务")
			}
			if final.Accepted+final.Full+final.EncodingBusy != uint64(1024+boolMeasure(gate != nil)) {
				t.Fatal("接受与拒绝统计不覆盖实际调用")
			}
			if mode == "error" && final.Outputs[0].Failed != final.Accepted {
				t.Fatal("持续写错必须逐项可见")
			}
			sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
			data := map[string]any{"mode": mode, "go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "cpu_count": runtime.NumCPU(), "config": c, "producers": 4, "calls": len(samples), "event_bytes": len(encoded), "elapsed_ns": duration.Nanoseconds(), "throughput_calls_per_second": float64(len(samples)) / duration.Seconds(), "process_cpu_seconds": cpu, "p50_ns": measurePercentile(samples, 50), "p95_ns": measurePercentile(samples, 95), "p99_ns": measurePercentile(samples, 99), "observed_peak_entries": peakEntries, "observed_peak_bytes": peakBytes, "observed_peak_encoders": peakEncoders, "observed_heap_alloc_peak": peakAlloc, "heap_before": before.Alloc, "heap_after": after.Alloc, "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "budget_pending_entries": pendingAtBudget, "budget_pending_bytes": bytesAtBudget, "final": final}
			p, err := json.Marshal(data)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("MEASUREMENT %s", p)
		})
	}
}

// 将受控首项的存在转换为调用数量，不把日志副本算成业务事件。
func boolMeasure(value bool) int {
	if value {
		return 1
	}
	return 0
}
