// 本文件检查队列按先进先出顺序取值，以及多个生产者同时写入时不丢失、重复或打乱各自顺序。
// 并发测试使用有限任务并等待全部生产者退出；只有一个消费者，不启动无限演示循环。
package cqueue

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	num := 3

	q := NewQueue()
	for i := range num {
		q.Push(i)
	}

	for range num {
		fmt.Println(q.Pop())
	}

}

func BenchmarkNewFIFOQueue(b *testing.B) {
	q := NewQueue()
	for i := range 1000000 {
		q.Push(i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.Push(i)
		q.Pop()
	}
}

// 四个生产者各写入100个唯一编号，单消费者核对完整数量和每个生产者的写入顺序。
// 超时只防止测试挂死；失败和成功均等待有限生产任务结束，不能遗留后台循环。
func TestQueuePop(t *testing.T) {
	const producerCount, valuesPerProducer = 4, 100
	q := NewQueue()
	var producers sync.WaitGroup
	start := make(chan struct{})
	for producer := range producerCount {
		producers.Add(1)
		go func() {
			defer producers.Done()
			<-start
			for sequence := range valuesPerProducer {
				q.Push(producer*valuesPerProducer + sequence)
			}
		}()
	}
	defer producers.Wait()
	close(start)
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	var nextSequence [producerCount]int
	for received := 0; received < producerCount*valuesPerProducer; {
		select {
		case <-deadline.C:
			t.Fatalf("只取到%d个编号，期望400个", received)
		default:
		}
		value := q.Pop()
		if value == nil {
			runtime.Gosched()
			continue
		}
		id, ok := value.(int)
		if !ok || id < 0 || id >= producerCount*valuesPerProducer {
			t.Fatalf("收到非法编号：%v", value)
		}
		producer, sequence := id/valuesPerProducer, id%valuesPerProducer
		if sequence != nextSequence[producer] {
			t.Fatalf("生产者%d的编号重复或乱序：实际%d，期望%d", producer, sequence, nextSequence[producer])
		}
		nextSequence[producer]++
		received++
	}
	producers.Wait()
	if q.Pop() != nil || !q.Empty() {
		t.Fatal("完整取出400个编号后队列仍有多余内容")
	}
}
