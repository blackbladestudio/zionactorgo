// 本文件验证雪花编号的构造边界、生成和格式转换；编号范围由当前NodeBits决定。
// JSON错误比较保留具体类型和完整原输入检查，不直接比较含字节切片的error值。
package csnowflake

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestPrintID(t *testing.T) {
	node, err := NewNode(1)
	if err != nil {
		t.Log(err)
		return
	}

	for range 10000 {
		id := node.Generate()
		fmt.Println(id.Int64(), id.Base58())
	}
}

//******************************************************************************
// General Test funcs

// 允许当前配置中的首尾节点编号，拒绝负数和超出NodeBits范围的编号。
func TestNewNode(t *testing.T) {
	maxNodeID := int64(1<<NodeBits) - 1
	for _, nodeID := range []int64{0, maxNodeID} {
		if _, err := NewNode(nodeID); err != nil {
			t.Fatalf("合法节点编号%d被拒绝：%v", nodeID, err)
		}
	}
	for _, nodeID := range []int64{-1, maxNodeID + 1} {
		if _, err := NewNode(nodeID); err == nil {
			t.Fatalf("非法节点编号%d没有被拒绝", nodeID)
		}
	}
}

// lazy check if Generate will create duplicate IDs
// would be good to later enhance this with more smarts
func TestGenerateDuplicateID(t *testing.T) {

	node, _ := NewNode(1)

	var x, y ID
	for range 1000000 {
		y = node.Generate()
		if x == y {
			t.Errorf("x(%d) & y(%d) are the same", x, y)
		}
		x = y
	}
}

// I feel like there's probably a better way
func TestRace(t *testing.T) {

	node, _ := NewNode(1)

	go func() {
		for range 1000000000 {
			NewNode(1)
		}
	}()

	for range 4000 {

		node.Generate()
	}

}

func TestAtomicInt64(t *testing.T) {
	nodeID := "game-123"
	nodeIDCRC32 := CRC32(nodeID)

	t.Logf("nodeIDCRC32 : %#v", nodeIDCRC32)
	t.Logf("nodeIDCRC32 binary : %#v", strconv.FormatInt(int64(nodeIDCRC32), 2))

	atomicID := time.Now().Unix()
	t.Logf("atomicID : %#v", atomicID)
	t.Logf("atomicID binary : %#v", strconv.FormatInt(atomicID, 2))

	//1100 0000 0001 1100 0110 1111 0010 110

	for range 10 {
		newID := atomic.AddInt64(&atomicID, 1)
		t.Logf("newid : %#v", newID)
	}
}

func CRC32(str string) uint32 {
	return crc32.ChecksumIEEE([]byte(str))
}

//******************************************************************************
// Converters/Parsers Test funcs
// We should have funcs here to test conversion both ways for everything

func TestPrintAll(t *testing.T) {
	node, err := NewNode(0)
	if err != nil {
		t.Fatalf("error creating NewNode, %s", err)
	}

	id := node.Generate()

	t.Logf("Int64    : %#v", id.Int64())
	t.Logf("String   : %#v", id.String())
	t.Logf("Base2    : %#v", id.Base2())
	t.Logf("Base32   : %#v", id.Base32())
	t.Logf("Base36   : %#v", id.Base36())
	t.Logf("Base58   : %#v", id.Base58())
	t.Logf("Base64   : %#v", id.Base64())
	t.Logf("Bytes    : %#v", id.Bytes())
	t.Logf("IntBytes : %#v", id.IntBytes())

}

func TestInt64(t *testing.T) {
	node, err := NewNode(0)
	if err != nil {
		t.Fatalf("error creating NewNode, %s", err)
	}

	oID := node.Generate()
	i := oID.Int64()

	pID := ParseInt64(i)
	if pID != oID {
		t.Fatalf("pID %v != oID %v", pID, oID)
	}

	mi := int64(1116766490855473152)
	pID = ParseInt64(mi)
	if pID.Int64() != mi {
		t.Fatalf("pID %v != mi %v", pID.Int64(), mi)
	}

}

func TestString(t *testing.T) {
	node, err := NewNode(0)
	if err != nil {
		t.Fatalf("error creating NewNode, %s", err)
	}

	oID := node.Generate()
	si := oID.String()

	pID, err := ParseString(si)
	if err != nil {
		t.Fatalf("error parsing, %s", err)
	}

	if pID != oID {
		t.Fatalf("pID %v != oID %v", pID, oID)
	}

	ms := `1116766490855473152`
	_, err = ParseString(ms)
	if err != nil {
		t.Fatalf("error parsing, %s", err)
	}

	ms = `1112316766490855473152`
	_, err = ParseString(ms)
	if err == nil {
		t.Fatalf("no error parsing %s", ms)
	}
}

func TestBase2(t *testing.T) {
	node, err := NewNode(0)
	if err != nil {
		t.Fatalf("error creating NewNode, %s", err)
	}

	oID := node.Generate()
	i := oID.Base2()

	pID, err := ParseBase2(i)
	if err != nil {
		t.Fatalf("error parsing, %s", err)
	}
	if pID != oID {
		t.Fatalf("pID %v != oID %v", pID, oID)
	}

	ms := `111101111111101110110101100101001000000000000000000000000000`
	_, err = ParseBase2(ms)
	if err != nil {
		t.Fatalf("error parsing, %s", err)
	}

	ms = `1112316766490855473152`
	_, err = ParseBase2(ms)
	if err == nil {
		t.Fatalf("no error parsing %s", ms)
	}
}

func TestBase32(t *testing.T) {

	node, err := NewNode(0)
	if err != nil {
		t.Fatalf("error creating NewNode, %s", err)
	}

	for range 100 {

		sf := node.Generate()
		b32i := sf.Base32()
		psf, err := ParseBase32([]byte(b32i))
		if err != nil {
			t.Fatal(err)
		}
		if sf != psf {
			t.Fatal("Parsed does not match String.")
		}
	}
}

func TestBase36(t *testing.T) {
	node, err := NewNode(0)
	if err != nil {
		t.Fatalf("error creating NewNode, %s", err)
	}

	oID := node.Generate()
	i := oID.Base36()

	pID, err := ParseBase36(i)
	if err != nil {
		t.Fatalf("error parsing, %s", err)
	}
	if pID != oID {
		t.Fatalf("pID %v != oID %v", pID, oID)
	}

	ms := `8hgmw4blvlkw`
	_, err = ParseBase36(ms)
	if err != nil {
		t.Fatalf("error parsing, %s", err)
	}

	ms = `68h5gmw443blv2lk1w`
	_, err = ParseBase36(ms)
	if err == nil {
		t.Fatalf("no error parsing, %s", err)
	}
}

func TestBase58(t *testing.T) {

	node, err := NewNode(0)
	if err != nil {
		t.Fatalf("error creating NewNode, %s", err)
	}

	for range 10 {

		sf := node.Generate()
		b58 := sf.Base58()
		psf, err := ParseBase58([]byte(b58))
		if err != nil {
			t.Fatal(err)
		}
		if sf != psf {
			t.Fatal("Parsed does not match String.")
		}
	}
}

func TestBase64(t *testing.T) {
	node, err := NewNode(0)
	if err != nil {
		t.Fatalf("error creating NewNode, %s", err)
	}

	oID := node.Generate()
	i := oID.Base64()

	pID, err := ParseBase64(i)
	if err != nil {
		t.Fatalf("error parsing, %s", err)
	}
	if pID != oID {
		t.Fatalf("pID %v != oID %v", pID, oID)
	}

	ms := `MTExNjgxOTQ5NDY2MDk5NzEyMA==`
	_, err = ParseBase64(ms)
	if err != nil {
		t.Fatalf("error parsing, %s", err)
	}

	ms = `MTExNjgxOTQ5NDY2MDk5NzEyMA`
	_, err = ParseBase64(ms)
	if err == nil {
		t.Fatalf("no error parsing, %s", err)
	}
}

func TestBytes(t *testing.T) {
	node, err := NewNode(0)
	if err != nil {
		t.Fatalf("error creating NewNode, %s", err)
	}

	oID := node.Generate()
	i := oID.Bytes()

	pID, err := ParseBytes(i)
	if err != nil {
		t.Fatalf("error parsing, %s", err)
	}
	if pID != oID {
		t.Fatalf("pID %v != oID %v", pID, oID)
	}

	ms := []byte{0x31, 0x31, 0x31, 0x36, 0x38, 0x32, 0x31, 0x36, 0x37, 0x39, 0x35, 0x37, 0x30, 0x34, 0x31, 0x39, 0x37, 0x31, 0x32}
	_, err = ParseBytes(ms)
	if err != nil {
		t.Fatalf("error parsing, %#v", err)
	}

	ms = []byte{0xFF, 0xFF, 0xFF, 0x31, 0x31, 0x31, 0x36, 0x38, 0x32, 0x31, 0x36, 0x37, 0x39, 0x35, 0x37, 0x30, 0x34, 0x31, 0x39, 0x37, 0x31, 0x32}
	_, err = ParseBytes(ms)
	if err == nil {
		t.Fatalf("no error parsing, %#v", err)
	}
}

func TestIntBytes(t *testing.T) {
	node, err := NewNode(0)
	if err != nil {
		t.Fatalf("error creating NewNode, %s", err)
	}

	oID := node.Generate()
	i := oID.IntBytes()

	pID := ParseIntBytes(i)
	if pID != oID {
		t.Fatalf("pID %v != oID %v", pID, oID)
	}

	ms := [8]uint8{0xf, 0x7f, 0xc0, 0xfc, 0x2f, 0x80, 0x0, 0x0}
	mi := int64(1116823421972381696)
	pID = ParseIntBytes(ms)
	if pID.Int64() != mi {
		t.Fatalf("pID %v != mi %v", pID.Int64(), mi)
	}

}

//******************************************************************************
// Marshall Test Methods

func TestMarshalJSON(t *testing.T) {
	id := ID(13587)
	expected := "\"13587\""

	jsonBytes, err := id.MarshalJSON()
	if err != nil {
		t.Fatalf("Unexpected error during MarshalJSON")
	}

	if string(jsonBytes) != expected {
		t.Fatalf("Got %s, expected %s", string(jsonBytes), expected)
	}
}

func TestMarshalsIntBytes(t *testing.T) {
	id := ID(13587).IntBytes()
	expected := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x35, 0x13}
	if !bytes.Equal(id[:], expected) {
		t.Fatalf("Expected ID to be encoded as %v, got %v", expected, id)
	}
}

// 合法JSON返回编号；不完整JSON保留语法错误类型及收到的完整字节，不触发error比较panic。
func TestUnmarshalJSON(t *testing.T) {
	tt := []struct {
		json        string
		expectedID  ID
		expectedErr error
	}{
		{`"13587"`, 13587, nil},
		{`1`, 0, JSONSyntaxError{[]byte("1")}},
		{`"invalid`, 0, JSONSyntaxError{[]byte(`"invalid`)}},
	}

	for _, tc := range tt {
		var id ID
		err := id.UnmarshalJSON([]byte(tc.json))

		if tc.expectedErr == nil {
			if err != nil {
				t.Fatalf("合法JSON返回错误：%v", err)
			}
		} else {
			var syntaxError JSONSyntaxError
			if !errors.As(err, &syntaxError) || !bytes.Equal(syntaxError.original, []byte(tc.json)) {
				t.Fatalf("语法错误未保留类型或完整原输入：%v", err)
			}
		}

		if id != tc.expectedID {
			t.Fatalf("Expected to get ID '%s' decoding JSON, but got '%s'", tc.expectedID, id)
		}
	}
}

func TestDefaultNode(t *testing.T) {
	InitDefaultNode("0")

	for range 10 {
		fmt.Println(NextID())
	}
}

// ****************************************************************************
// Benchmark Methods

func BenchmarkParseBase32(b *testing.B) {

	node, _ := NewNode(1)
	sf := node.Generate()
	b32i := sf.Base32()

	b.ReportAllocs()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		ParseBase32([]byte(b32i))
	}
}
func BenchmarkBase32(b *testing.B) {

	node, _ := NewNode(1)
	sf := node.Generate()

	b.ReportAllocs()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sf.Base32()
	}
}
func BenchmarkParseBase58(b *testing.B) {

	node, _ := NewNode(1)
	sf := node.Generate()
	b58 := sf.Base58()

	b.ReportAllocs()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		ParseBase58([]byte(b58))
	}
}
func BenchmarkBase58(b *testing.B) {

	node, _ := NewNode(1)
	sf := node.Generate()

	b.ReportAllocs()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sf.Base58()
	}
}
func BenchmarkGenerate(b *testing.B) {

	node, _ := NewNode(1)

	b.ReportAllocs()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = node.Generate()
	}
}

func BenchmarkGenerateMaxSequence(b *testing.B) {

	NodeBits = 1
	StepBits = 21
	node, _ := NewNode(1)

	b.ReportAllocs()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = node.Generate()
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	// Generate the ID to unmarshal
	node, _ := NewNode(1)
	id := node.Generate()
	jsonBytes, _ := id.MarshalJSON()

	var id2 ID

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = id2.UnmarshalJSON(jsonBytes)
	}
}

func BenchmarkMarshal(b *testing.B) {
	// Generate the ID to marshal
	node, _ := NewNode(1)
	id := node.Generate()

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, _ = id.MarshalJSON()
	}
}
