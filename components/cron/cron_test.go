// 本文件验证每天、每小时和固定间隔的任务登记；用给定日期计算下一次执行时刻。
// 间隔任务另启动真实调度器，收到一次回调后停止并等待收尾；不进入一直等待的前台Run。
package ccron

import (
	"testing"
	"time"
)

// 每天与每小时规则在17:32:04检查时，应指向17:32:05；间隔任务还须实际触发。
// 测试独占本包调度器，结束时先停新实例、等待回调结束，再恢复原全局实例。
func TestAddEveryDayFunc(t *testing.T) {
	previous := _cron
	Init()
	owned := _cron
	defer func() {
		closed := owned.Stop()
		select {
		case <-closed.Done():
		case <-time.After(3 * time.Second):
			t.Error("调度器未在收尾期限内结束")
		}
		_cron = previous
	}()
	dayID, err := AddEveryDayFunc(func() {}, 17, 32, 5)
	if err != nil {
		t.Fatal(err)
	}
	hourID, err := AddEveryHourFunc(func() {}, 32, 5)
	if err != nil {
		t.Fatal(err)
	}
	anchor := time.Date(2026, 1, 2, 17, 32, 4, 0, Location())
	for _, entry := range []struct {
		name string    // 本次验证的时间规则。
		next time.Time // 指定检查时间之后的实际下一次执行时刻。
	}{
		{"每天", Entry(dayID).Schedule.Next(anchor)},
		{"每小时", Entry(hourID).Schedule.Next(anchor)},
	} {
		if !entry.next.Equal(anchor.Add(time.Second)) {
			t.Fatalf("%s任务的下一次时刻=%v，期望17:32:05", entry.name, entry.next)
		}
	}
	fired := make(chan struct{}, 1)
	_, err = AddDurationFunc(func() {
		select {
		case fired <- struct{}{}:
		default:
		}
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	Start()
	select {
	case <-fired:
	case <-time.After(3 * time.Second):
		t.Fatal("实际间隔任务没有触发")
	}
}
