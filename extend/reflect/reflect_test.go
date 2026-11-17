// 本文件检查反射辅助接口识别函数名和指针；非函数输入应按现有契约触发panic。
// 测试负责捕获预期拒绝，不修改生产接口，也不把未捕获panic当作验证成功。
package creflect

import (
	"fmt"
	"testing"
)

type FunStruct struct {
}

func (f *FunStruct) A() string {
	return "A method"
}

func test1111() {}

// 核对普通函数与绑定方法的名称，并明确验证非函数参数被拒绝。
func TestGetFuncName(t *testing.T) {
	f := &FunStruct{}

	t.Run("拒绝非函数", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("非函数输入没有触发预期拒绝")
			}
		}()
		GetFuncName(f)
	})
	if name := GetFuncName(f.A); name != "A" {
		t.Fatalf("绑定方法名=%q，期望A", name)
	}
	if name := GetFuncName(test1111); name != "test1111" {
		t.Fatalf("普通函数名=%q，期望test1111", name)
	}
}

func TestIsPtr(t *testing.T) {
	fmt.Println(IsPtr(nil))
}

func TestIsNotPtr(t *testing.T) {
	s := &FunStruct{}
	fmt.Println(IsNotPtr(s))
}
