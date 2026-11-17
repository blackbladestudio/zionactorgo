// 本文件检查时间轮的单次类型任务已触发、重复任务至少触发两次，然后停止时间轮并等待内部任务退出。
// 使用真实短间隔及三秒防挂死期限，不连接业务服务，也不把固定睡眠一小时当作验收。
package ctimewheel

import (
	"testing"
	"time"
)

// 回调只发送不阻塞通知；单次回调或两次重复回调缺失则失败，所有结束路径都停止本轮时间轮。
func TestAddFunc(t *testing.T) {
	tw := NewTimeWheel(time.Millisecond, 20)
	if tw == nil {
		t.Fatal("合法配置没有创建时间轮")
	}
	tw.Start()
	defer tw.Stop()
	onceFired, repeatedFired := make(chan struct{}, 1), make(chan struct{}, 2)
	tw.AfterFunc(NextID(), 10*time.Millisecond, func() {
		select {
		case onceFired <- struct{}{}:
		default:
		}
	}, false)
	tw.AddEveryFunc(NextID(), 20*time.Millisecond, func() {
		select {
		case repeatedFired <- struct{}{}:
		default:
		}
	}, false)
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for _, fired := range []<-chan struct{}{onceFired, repeatedFired, repeatedFired} {
		select {
		case <-fired:
		case <-deadline.C:
			t.Fatal("单次或重复任务没有在期限内实际触发")
		}
	}
}
