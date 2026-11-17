// 本文件验证日志统计改名前后的副本、序列化和匿名字段契约，依赖同包测试出口。
// 测试不连接真实服务；类型名字的授权变化由阶段工具另行核对。
package clogger

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

// 用确定统计值核对JSON展开和出口副本，避免后台时序改变比较输入。
func TestAsyncStatsRenameContract(t *testing.T) {
	a, err := NewAsyncLogger(validAsyncConfig(), testOutputs(&memoryOutput{}))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	a.mu.Lock()
	a.state.Accepted = 3
	a.state.Outputs[0].Failed = 2
	a.mu.Unlock()
	s := a.Stats()
	before, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	report := a.v2Report("invalid_context")
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	var stats map[string]json.RawMessage
	if err = json.Unmarshal(before, &stats); err != nil {
		t.Fatal(err)
	}
	if len(fields) != len(stats)+2 {
		t.Fatalf("匿名字段未按统计成员展开: %s", raw)
	}
	for name, value := range stats {
		if string(fields[name]) != string(value) {
			t.Fatalf("报告统计值变化: %s", name)
		}
	}
	s.Accepted = 99
	s.Outputs[0].Failed = 99
	if next := a.Stats(); next.Accepted != 3 || next.Outputs[0].Failed != 2 {
		t.Fatal("调用者修改副本影响内部状态")
	}
	a.mu.Lock()
	a.state.Accepted = 4
	a.state.Outputs[0].Failed = 5
	a.mu.Unlock()
	if report.Accepted != 3 || report.Outputs[0].Failed != 2 {
		t.Fatal("后续内部变化影响已取得报告")
	}
	typeInfo := reflect.TypeOf(report)
	if !typeInfo.Field(0).Anonymous || typeInfo.Field(0).Name != reflect.TypeOf(a.Stats()).Name() {
		t.Fatal("匿名字段名称与统计类型不一致")
	}
	t.Logf("统计类型=%s 出口类型=%s 匿名字段=%s StatsJSON=%s CloseJSON=%s", reflect.TypeOf(a.Stats()).Name(), reflect.TypeOf(a.Stats().Outputs[0]).Name(), typeInfo.Field(0).Name, before, raw)
}
