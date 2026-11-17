// 本文件定义独立日志实例的服务身份、字段允许清单、输出目标和容量，并校验创建配置。
// 调用者提供文件或标准输出等目标的Writer（写出和刷新接口），用NewAsyncLogger创建实例后投递事件、读取结果。
// 本文件还定义投递结果、统计副本和关闭报告；后台写出由async_logger.go负责，接收与统计不证明持久保存。
// 调用者负责中止阻塞写出并最终关闭输出目标；容量必须显式设置，分别限制输入长度、活跃编码数量、排队和写中事件的条数与内容字节，不是进程内存硬限。
package clogger

import (
	"errors"
	"reflect"
	"strings"
	"unicode/utf8"
)

var errAsyncConfig = errors.New("受管日志配置或出口不合法")

// 检查小写ASCII键的语法和字节上限；保留身份键由调用者另行检查。
func validAsyncKey(key string, limit int) bool {
	if len(key) == 0 || len(key) > limit || key[0] < 'a' || key[0] > 'z' {
		return false
	}
	for i := 1; i < len(key); i++ {
		b := key[i]
		if (b < 'a' || b > 'z') && (b < '0' || b > '9') && b != '_' {
			return false
		}
	}
	return true
}

// 检查身份和负责人文字，拒绝非法UTF-8和可改变物理行的控制字符。
func validAsyncText(value string, limit int) bool {
	if value == "" || len(value) > limit || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}

// 只解释四种创建和事件等级，不继承旧GetLevel的静默回退。
func asyncLevel(value string) (int, bool) {
	switch strings.ToLower(value) {
	case "debug":
		return 0, true
	case "info":
		return 1, true
	case "warn":
		return 2, true
	case "error":
		return 3, true
	}
	return 0, false
}

// 先限制容器和容量再复制配置，失败时不创建后台任务或探测Writer。
func validateAsyncConfig(c AsyncConfig, outputs []AsyncOutput) (AsyncConfig, []AsyncOutput, error) {
	if _, ok := asyncLevel(c.Level); !ok {
		return c, nil, errAsyncConfig
	}
	if !validAsyncText(c.Service, 128) || !validAsyncText(c.Instance, 128) || !validAsyncText(c.Version, 128) || len(c.AllowedFields) > 32 || len(outputs) < 1 || len(outputs) > 4 {
		return c, nil, errAsyncConfig
	}
	limits := [][2]int{{c.MaxPairs, 32}, {c.MaxKeyBytes, 64}, {c.MaxStringBytes, 4096}, {c.MaxMessageBytes, 4096}, {c.MaxEventBytes, 65536}, {c.MaxEntries, 65536}, {c.MaxBytes, 268435456}, {c.MaxEncoders, 64}}
	for _, limit := range limits {
		if limit[0] < 1 || limit[0] > limit[1] {
			return c, nil, errAsyncConfig
		}
	}
	if c.MaxEventBytes > c.MaxBytes {
		return c, nil, errAsyncConfig
	}
	fields := make(map[string]FieldKind, len(c.AllowedFields))
	for key, kind := range c.AllowedFields {
		if !validAsyncKey(key, c.MaxKeyBytes) || reservedAsyncKey(key) {
			return c, nil, errAsyncConfig
		}
		switch kind {
		case StringField, BoolField, SignedField, UnsignedField:
		default:
			return c, nil, errAsyncConfig
		}
		fields[key] = kind
	}
	seen := make(map[string]bool, len(outputs))
	for _, output := range outputs {
		if !validAsyncKey(output.Name, 64) || seen[output.Name] || !validAsyncText(output.Owner, 128) || nilAsyncWriter(output.Writer) {
			return c, nil, errAsyncConfig
		}
		seen[output.Name] = true
	}
	c.AllowedFields = fields
	return c, append([]AsyncOutput(nil), outputs...), nil
}

// 这些键由实例或编码器生成，业务允许清单和事件均不得使用。
func reservedAsyncKey(key string) bool {
	switch key {
	case "ts", "level", "msg", "service", "instance", "version":
		return true
	}
	return false
}

// 判断接口或其可为空的底层值是否为nil，不调用Writer方法或反射编码对象。
func nilAsyncWriter(writer AsyncWriter) bool {
	if writer == nil {
		return true
	}
	value := reflect.ValueOf(writer)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	}
	return false
}

// FieldKind 规定允许字段的内建值类别，不允许任意对象自动转换成文字。
type FieldKind string

const (
	StringField   FieldKind = "string"
	BoolField     FieldKind = "bool"
	SignedField   FieldKind = "signed"
	UnsignedField FieldKind = "unsigned"
)

// AsyncConfig 规定独立日志实例的身份、允许字段和正数容量；构造时复制，旧全局日志不受影响。
type AsyncConfig struct {
	Level           string               // debug/info/warn/error之一，不接受空白或未知等级。
	Service         string               // 服务名称，最多128个UTF-8字节。
	Instance        string               // 区分本次启动的身份，最多128个UTF-8字节。
	Version         string               // 当前程序版本，最多128个UTF-8字节。
	AllowedFields   map[string]FieldKind // 业务批准的字段和类型，构造时复制，最多32项。
	MaxPairs        int                  // 单条业务字段上限，最多32项。
	MaxKeyBytes     int                  // 单个ASCII键上限，最多64字节。
	MaxStringBytes  int                  // 单个业务字符串上限，最多4096字节。
	MaxMessageBytes int                  // 消息上限，最多4096字节。
	MaxEventBytes   int                  // 单条完整编码输出上限，最多65536字节，不是编码缓冲或进程堆内存上限。
	MaxEntries      int                  // 排队和正在写出的总条数，最多65536条。
	MaxBytes        int                  // 排队和正在写出的内容总字节，最多256MiB。
	MaxEncoders     int                  // 同时验证和编码的调用数，最多64个。
}

// AsyncWriter 接收一次完整事件；调用者须提供期限或中止机制并负责最终释放。
type AsyncWriter interface {
	Write([]byte) (int, error) // 返回本次写出字节和错误，组件不循环补写。
	Sync() error               // 声明当前出口的刷新层级，不能据此自动推断持久化。
}

// AsyncOutput 固定一个出口及其清理责任，组件借用Writer而不关闭它。
type AsyncOutput struct {
	Name   string      // 唯一小写ASCII出口名，最多64字节。
	Owner  string      // 谁中止和释放出口，最多128个UTF-8字节。
	Writer AsyncWriter // 必须非nil且提供实际声明的Sync行为。
}

// Result 表示本次投递的准入结果；accepted只表示接收，未保证写出或持久保存。
// Error仅在invalid时提供固定安全原因；其他拒绝由Status区分，不携带出口原错误。
type Result struct {
	Status string // accepted/filtered/invalid/full/encoding_busy/closed。
	Error  error  // 拒绝原因；不携带后台Writer的任意错误对象。
}

// LogOutputStatsSnapshot 是Stats持锁读取时取得的单个出口累计统计副本。
// 调用者修改不会影响内部状态，后台变化也不会更新此副本；不保存原错误对象。
type LogOutputStatsSnapshot struct {
	Name        string // 构造时登记的出口名。
	Succeeded   uint64 // 完整Write且无错的次数。
	Failed      uint64 // 写出失败次数，包括非法n和短写。
	ShortWrites uint64 // 0<=n<len的次数，带错也计入。
	SyncFailed  uint64 // 刷新失败次数。
	LastFailure string // 最近固定失败类别，绝不调用任意Error方法。
}

// LogStatsSnapshot 是Stats持锁读取时取得的队列、接收、拒绝和输出失败统计副本。
// Outputs也独立复制；副本不会实时更新，不表示持久保存，不向输出队列写诊断。
type LogStatsSnapshot struct {
	Closed         bool                     // 已封准入，不再接受新事件。
	EncodersActive int                      // 仍在验证或编码的调用数。
	PendingEntries int                      // 含正在Write的已接收条数。
	PendingBytes   int                      // 含正在Write的独立内容字节。
	Accepted       uint64                   // 接收事件数，不代表保存完成。
	Filtered       uint64                   // 合法但低于创建等级的事件数。
	Invalid        uint64                   // 输入被拒绝的次数。
	Full           uint64                   // 队列条数或字节不足的次数。
	EncodingBusy   uint64                   // 编码额度不足的次数。
	ClosedRejected uint64                   // 关闭拒绝次数。
	Completed      uint64                   // 所有出口本次Write尝试均返回的事件数。
	Succeeded      uint64                   // 所有出口完整Write成功的事件数。
	FlushCompleted bool                     // 所有Sync都已返回，不代表都成功。
	FlushSucceeded bool                     // 所有Sync均无错误。
	WorkerExited   bool                     // 后台写出和刷新流程已结束，不代表活动编码也结束。
	Outputs        []LogOutputStatsSnapshot // 固定且最多四项的出口统计副本。
}

// CloseReport 包含返回报告时取得的统计副本及等待结果，不表示持久保存。
// finished表示后台及活动编码已结束，不保证各出口成功；其他结果只表示本次等待结束。
type CloseReport struct {
	LogStatsSnapshot        // 返回报告时取得的活动和失败统计副本。
	Drained          bool   // 已接收条数为零，刷新可能仍未结束。
	WaitStatus       string // finished/deadline/canceled/invalid_context。
}
