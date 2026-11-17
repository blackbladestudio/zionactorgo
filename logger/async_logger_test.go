// 本文件使用内存出口核对配置拒绝、安全字段和旧Console格式；不连接真实采集或存储服务。
// 新行为先在可编译接口上断言失败，所有实例均由测试关闭；出口本身不创建后台任务。
package clogger

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// 命名类型用于证明框架不会把任意业务类型自动解释成受支持标量。
type namedAsyncString string

// 安全陷阱对象在自动字符串转换时令测试失败。
type unsafeAsyncValue struct{ t *testing.T }

// String 不应被受管输入调用，因为对象不是批准的标量类型。
func (v unsafeAsyncValue) String() string {
	v.t.Fatal("不能调用任意String方法")
	return "secret"
}

// 使用原构造器和真实临时文件核对旧clog格式参数及Console物理行，测试后恢复全局出口。
func TestAsyncLegacyFormattedConsoleUnchanged(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "legacy-console")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	previousStderr, previousLogger := os.Stderr, DefaultLogger
	os.Stderr = file
	defer func() { os.Stderr = previousStderr; DefaultLogger = previousLogger }()
	DefaultLogger = NewConfigLogger(defaultConsoleConfig())
	Info("进度100%%，申请%s，人数%d", "ticket1", 3)
	if err := DefaultLogger.Sync(); err != nil {
		t.Fatal(err)
	}
	p, err := os.ReadFile(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(p), "INFO") || !strings.Contains(string(p), "进度100%，申请ticket1，人数3") || strings.Count(string(p), "\n") != 1 {
		t.Fatalf("旧格式化及Console输出改变: %q", p)
	}
}

// Error 不应被受管输入调用，因为错误文字可能包含敏感内容。
func (v unsafeAsyncValue) Error() string { v.t.Fatal("不能调用任意Error方法"); return "secret" }

// 验证合法实例的事件校验整条拒绝，不靠构造失败遮盖输入层缺失。
func TestAsyncEventValidation(t *testing.T) {
	cases := []struct {
		name, level, msg string
		pairs            []any
	}{
		{"odd", "info", "正常", []any{"event"}},
		{"nonstring", "info", "正常", []any{1, "event"}},
		{"duplicate", "info", "正常", []any{"event", "a", "event", "b"}},
		{"unknown", "info", "正常", []any{"secret", "a"}},
		{"reserved", "info", "正常", []any{"service", "a"}},
		{"wrong_kind", "info", "正常", []any{"event", int(1)}},
		{"named", "info", "正常", []any{"event", namedAsyncString("a")}},
		{"nil", "info", "正常", []any{"event", nil}},
		{"pointer", "info", "正常", []any{"event", new(string)}},
		{"nested", "info", "正常", []any{"event", map[string]string{"a": "b"}}},
		{"float", "info", "正常", []any{"count", 1.0}},
		{"error", "info", "正常", []any{"event", errors.New("秘密")}},
		{"object_methods", "info", "正常", []any{"event", unsafeAsyncValue{t}}},
		{"utf8", "info", "正常", []any{"event", "\xff"}},
		{"string_long", "info", "正常", []any{"event", strings.Repeat("a", 65)}},
		{"level", "bogus", "正常", nil},
		{"message_long", "info", strings.Repeat("a", 65), nil},
		{"message_utf8", "info", "\xff", nil},
		{"message_newline", "info", "a\nb", nil},
		{"message_tab", "info", "a\tb", nil},
		{"message_del", "info", "a\x7fb", nil},
		{"filtered_invalid", "debug", "正常", []any{"secret", "a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, err := NewAsyncLogger(validAsyncConfig(), testOutputs(&memoryOutput{}))
			if err != nil {
				t.Fatal("合法构造前提失败")
			}
			defer a.Close(context.Background())
			p, _, err := a.encodeEvent(tc.level, tc.msg, tc.pairs)
			if err == nil || len(p) != 0 {
				t.Fatal("事件必须整条拒绝并返回安全原因")
			}
		})
	}
}

// 验证精确Console字节边界和字段转义，合法换行不能生成第二个物理行。
func TestAsyncEncodedBoundaryAndCopy(t *testing.T) {
	c := validAsyncConfig()
	w := &memoryOutput{}
	a, err := NewAsyncLogger(c, testOutputs(w))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	pairs := []any{"event", "a\n\x01\"\\", "count", int64(-9223372036854775808), "room_id", uint64(18446744073709551615)}
	p, _, err := a.encodeEvent("INFO", "申请已登记", pairs)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(p), `"event": "a\n\u0001\"\\"`) || strings.Count(string(p), "\n") != 1 || !strings.Contains(string(p), `"service": "ms"`) {
		t.Fatalf("Console输出未保留安全字段或物理行: %q", p)
	}
	pairs[1] = "后来改动"
	c.AllowedFields["event"] = BoolField
	if strings.Contains(string(p), "后来改动") {
		t.Fatal("输出应独立于原参数")
	}
	// 直接固定实例的单条上限，不让配置复制测试改变事件内容；相同秒格式长度固定。
	a.config.MaxEventBytes = len(p)
	exact, _, err := a.encodeEvent("info", "申请已登记", []any{"event", "a\n\x01\"\\", "count", int64(-9223372036854775808), "room_id", uint64(18446744073709551615)})
	if err != nil || len(exact) != len(p) {
		t.Fatal("精确边界必须合法")
	}
	a.config.MaxEventBytes = len(p) - 1
	if _, _, err := a.encodeEvent("info", "申请已登记", []any{"event", "a\n\x01\"\\", "count", int64(-9223372036854775808), "room_id", uint64(18446744073709551615)}); err == nil {
		t.Fatal("编码长度超一字节必须拒绝")
	}
}

// 内存出口串行保存输出字节，供测试核对，不声明持久化。
type memoryOutput struct {
	mu   sync.Mutex
	data []byte
}

// Write 保存本次字节副本，调用者之后修改输入不影响已保存内容。
func (w *memoryOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.data = append(w.data, p...)
	return len(p), nil
}

// Sync 表示内存出口无需额外刷新，不代表数据已落盘。
func (w *memoryOutput) Sync() error { return nil }

// 为测试提供显式小容量，数值不作为生产推荐。
func validAsyncConfig() AsyncConfig {
	return AsyncConfig{Level: "info", Service: "ms", Instance: "boot1", Version: "test", AllowedFields: map[string]FieldKind{"event": StringField, "count": SignedField, "room_id": UnsignedField, "ok": BoolField}, MaxPairs: 8, MaxKeyBytes: 32, MaxStringBytes: 64, MaxMessageBytes: 64, MaxEventBytes: 1024, MaxEntries: 8, MaxBytes: 8192, MaxEncoders: 2}
}

// 给测试实例指定唯一出口及清理责任。
func testOutputs(w AsyncWriter) []AsyncOutput {
	return []AsyncOutput{{Name: "memory", Owner: "测试负责关闭实例", Writer: w}}
}

// 验证构造在启动任何任务前拒绝非法配置，不让默认值隐藏错误。
func TestAsyncConstructorRejectsInvalid(t *testing.T) {
	cases := []struct {
		name   string
		change func(*AsyncConfig)
	}{
		{"level", func(c *AsyncConfig) { c.Level = "bogus" }},
		{"level_space", func(c *AsyncConfig) { c.Level = " info" }},
		{"service", func(c *AsyncConfig) { c.Service = "" }},
		{"identity_long", func(c *AsyncConfig) { c.Version = strings.Repeat("v", 129) }},
		{"identity_utf8", func(c *AsyncConfig) { c.Instance = "\xff" }},
		{"pairs_zero", func(c *AsyncConfig) { c.MaxPairs = 0 }},
		{"pairs_high", func(c *AsyncConfig) { c.MaxPairs = 33 }},
		{"key_high", func(c *AsyncConfig) { c.MaxKeyBytes = 65 }},
		{"string_high", func(c *AsyncConfig) { c.MaxStringBytes = 4097 }},
		{"message_high", func(c *AsyncConfig) { c.MaxMessageBytes = 4097 }},
		{"event_high", func(c *AsyncConfig) { c.MaxEventBytes = 65537 }},
		{"entries_high", func(c *AsyncConfig) { c.MaxEntries = 65537 }},
		{"bytes_high", func(c *AsyncConfig) { c.MaxBytes = 268435457 }},
		{"encoders_high", func(c *AsyncConfig) { c.MaxEncoders = 65 }},
		{"event_over_bytes", func(c *AsyncConfig) { c.MaxBytes = 1 }},
		{"reserved", func(c *AsyncConfig) { c.AllowedFields["service"] = StringField }},
		{"uppercase_key", func(c *AsyncConfig) { c.AllowedFields["Event"] = StringField }},
		{"type", func(c *AsyncConfig) { c.AllowedFields["event"] = "object" }},
		{"allowlist_high", func(c *AsyncConfig) {
			for i := 0; i < 33; i++ {
				c.AllowedFields[strings.Repeat("a", i+1)] = StringField
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validAsyncConfig()
			tc.change(&c)
			a, err := NewAsyncLogger(c, testOutputs(&memoryOutput{}))
			if a != nil {
				a.Close(context.Background())
				t.Fatal("非法配置不得返回实例")
			}
			if err == nil {
				t.Fatal("非法配置必须在构造时拒绝")
			}
		})
	}
}

// 验证出口无效时不创建实例，避免运行后才发现无法写出或清理。
func TestAsyncConstructorRejectsOutputs(t *testing.T) {
	var typedNil *memoryOutput
	cases := []struct {
		name    string
		outputs []AsyncOutput
	}{
		{"empty", nil}, {"nil", testOutputs(nil)}, {"typed_nil", testOutputs(typedNil)},
		{"duplicate", append(testOutputs(&memoryOutput{}), testOutputs(&memoryOutput{})...)},
		{"owner", []AsyncOutput{{Name: "memory", Writer: &memoryOutput{}}}},
		{"owner_control", []AsyncOutput{{Name: "memory", Owner: "bad\nowner", Writer: &memoryOutput{}}}},
		{"name", []AsyncOutput{{Name: "Memory", Owner: "测试", Writer: &memoryOutput{}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, err := NewAsyncLogger(validAsyncConfig(), tc.outputs)
			if a != nil {
				a.Close(context.Background())
				t.Fatal("非法出口不得返回实例")
			}
			if err == nil {
				t.Fatal("非法出口必须拒绝")
			}
		})
	}
}

// 检查全部资源正值与硬上限；这些已有正确校验直接验证，不人为破坏实现制造失败。
func TestAsyncAllResourceBounds(t *testing.T) {
	resources := []struct {
		name    string
		set     func(*AsyncConfig, int)
		maximum int
	}{
		{"pairs", func(c *AsyncConfig, n int) { c.MaxPairs = n }, 32}, {"key", func(c *AsyncConfig, n int) { c.MaxKeyBytes = n }, 64},
		{"string", func(c *AsyncConfig, n int) { c.MaxStringBytes = n }, 4096}, {"message", func(c *AsyncConfig, n int) { c.MaxMessageBytes = n }, 4096},
		{"event", func(c *AsyncConfig, n int) { c.MaxEventBytes = n }, 65536}, {"entries", func(c *AsyncConfig, n int) { c.MaxEntries = n }, 65536},
		{"bytes", func(c *AsyncConfig, n int) { c.MaxBytes = n }, 268435456}, {"encoders", func(c *AsyncConfig, n int) { c.MaxEncoders = n }, 64},
	}
	for _, resource := range resources {
		for _, n := range []int{-1, 0, resource.maximum + 1} {
			t.Run(resource.name+"_"+strconv.Itoa(n), func(t *testing.T) {
				c := validAsyncConfig()
				resource.set(&c, n)
				a, err := NewAsyncLogger(c, testOutputs(&memoryOutput{}))
				if a != nil {
					a.Close(context.Background())
					t.Fatal("非法资源不得返回实例")
				}
				if err == nil {
					t.Fatal("非法资源必须返回错误")
				}
			})
		}
	}
	c := validAsyncConfig()
	for _, resource := range resources {
		resource.set(&c, resource.maximum)
	}
	a, err := NewAsyncLogger(c, testOutputs(&memoryOutput{}))
	if err != nil || a == nil {
		t.Fatal("所有硬上限本身应合法")
	}
	a.Close(context.Background())
}

// 构造后修改原清单和出口列表不能改写实例，只读统计也不能改写内部计数。
func TestAsyncConstructorCopiesContainers(t *testing.T) {
	c := validAsyncConfig()
	w := &memoryOutput{}
	outputs := testOutputs(w)
	a, err := NewAsyncLogger(c, outputs)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	c.AllowedFields["event"] = BoolField
	outputs[0].Name = "changed"
	outputs[0].Writer = nil
	if result := a.Infow("独立配置", "event", "registered"); result.Status != "accepted" {
		t.Fatal("外部允许清单修改不得影响实例")
	}
	r := a.Close(context.Background())
	if r.Outputs[0].Name != "memory" || r.Outputs[0].Succeeded != 1 {
		t.Fatal("出口列表须在构造时复制")
	}
	r.Outputs[0].Name = "改动统计"
	if a.Stats().Outputs[0].Name != "memory" {
		t.Fatal("统计副本不得改写内部状态")
	}
}

// 精确编码上限独立验证，明确此断言是在编码实现后增强，原首红没有到达它。
func TestAsyncExactEncodedLimit(t *testing.T) {
	a, err := NewAsyncLogger(validAsyncConfig(), testOutputs(&memoryOutput{}))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	pairs := []any{"event", "\n\x00\"\\", "room_id", uint64(18446744073709551615)}
	p, _, err := a.encodeEvent("info", "精确边界", pairs)
	if err != nil {
		t.Fatal(err)
	}
	a.config.MaxEventBytes = len(p)
	if p2, _, err := a.encodeEvent("info", "精确边界", pairs); err != nil || len(p2) != len(p) {
		t.Fatal("精确编码上限本身应合法")
	}
	a.config.MaxEventBytes = len(p) - 1
	if p2, _, err := a.encodeEvent("info", "精确边界", pairs); err == nil || len(p2) != 0 {
		t.Fatal("超过精确上限一字节必须整条拒绝")
	}
}

// 公开入口在等级过滤前完成输入和编码长度校验，非法日志不得调用Writer或占活动预算。
func TestAsyncPublicValidationAndFiltering(t *testing.T) {
	a, err := NewAsyncLogger(validAsyncConfig(), testOutputs(&memoryOutput{}))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	if r := a.Debugw("合法低级别", "event", "registered"); r.Status != "filtered" {
		t.Fatal("合法低等级应过滤")
	}
	if r := a.Debugw("非法低级别", "unknown", "secret"); r.Status != "invalid" || r.Error == nil {
		t.Fatal("非法字段不能被低等级隐藏")
	}
	a.config.MaxEventBytes = 10
	if r := a.Debugw("超限低级别"); r.Status != "invalid" {
		t.Fatal("编码总长检查必须先于过滤")
	}
	s := a.Stats()
	if s.Filtered != 1 || s.Invalid != 2 || s.Accepted != 0 || s.PendingEntries != 0 || s.PendingBytes != 0 || s.EncodersActive != 0 {
		t.Fatalf("拒绝和过滤不得占活动资源: %+v", s)
	}
}

// 验证合法构造可进入事件断言，不能用构造失败充当安全事件的红灯。
func TestAsyncValidEventAccepted(t *testing.T) {
	a, err := NewAsyncLogger(validAsyncConfig(), testOutputs(&memoryOutput{}))
	if err != nil || a == nil {
		t.Fatalf("合法前提不成立: %v", err)
	}
	defer a.Close(context.Background())
	if result := a.Infow("申请已登记", "event", "registered", "count", int64(3), "room_id", uint64(1), "ok", true); result.Status != "accepted" {
		t.Fatalf("合法事件应接收，得到%s", result.Status)
	}
}
