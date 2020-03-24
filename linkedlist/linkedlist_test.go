package linkedlist

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/xyproto/simplebolt"
)

type TestLL struct {
	*LinkedList
}

// NewTestDB returns a TestDB using a temporary path.
func NewTestLL() *TestLL {
	// Retrieve a temporary path.
	f, err := ioutil.TempFile("", "")
	if err != nil {
		panic("temp file: " + err.Error())
	}
	path := f.Name()
	f.Close()
	os.Remove(path)
	// Open the database.
	db, err := simplebolt.New(path)
	if err != nil {
		panic("open: " + err.Error())
	}
	ll, err := New(db, "tempLLname")
	if err != nil {
		panic("linkedlist: " + err.Error())
	}
	// Return wrapped type.
	return &TestLL{ll}
}

// Close and delete Bolt database.
func (ll *TestLL) Close() {
	defer os.Remove(ll.db.Path())
	ll.db.Close()
}

func TestGetters(t *testing.T) {
	ll := NewTestLL()
	defer ll.Close()

	data := [][]byte{
		[]byte("ABC"),
		[]byte("DEF"),
		[]byte("GHI"),
		[]byte("JKL"),
	}
	var err error
	for _, d := range data {
		err = ll.PushBack(d)
		ok(t, err)
	}
	front, err := ll.Front()
	ok(t, err)
	equals(t, []byte("ABC"), front.Data.Value())

	back, err := ll.Back()
	ok(t, err)
	equals(t, []byte("JKL"), back.Data.Value())

	def, err := ll.Get([]byte("DEF"))
	ok(t, err)
	equals(t, []byte("DEF"), def.Data.Value())

	next := front.Next()
	equals(t, next, def)

	notIn, err := ll.Get([]byte("XYZ"))
	ok(t, err)
	assert(t, notIn == nil, "Get expected nil")

	ghi, err := ll.GetNext([]byte("GHI"), def)
	ok(t, err)
	equals(t, []byte("GHI"), ghi.Data.Value())

	prev := back.Prev()
	equals(t, prev, ghi)

	def, err = ll.GetFunc([]byte("D"), getfunc)
	ok(t, err)
	equals(t, []byte("DEF"), def.Data.Value())

	notIn, err = ll.GetFunc([]byte("X"), getfunc)
	ok(t, err)
	assert(t, notIn == nil, "GetFunc expected nil")
}

func TestModifiers(t *testing.T) {
	ll := NewTestLL()
	defer ll.Close()

	data := [][]byte{
		[]byte("ABC"),
		[]byte("DEF"),
		[]byte("JKL"),
	}
	var err error
	for _, d := range data {
		err = ll.PushBack(d)
		ok(t, err)
	}
	back, err := ll.Back()
	ok(t, err)
	equals(t, []byte("JKL"), back.Data.Value())

	def, err := ll.Get([]byte("DEF"))
	ok(t, err)
	equals(t, []byte("DEF"), def.Data.Value())
	// Move DEF to front
	err = ll.MoveToFront(def)
	ok(t, err)

	front, err := ll.Front()
	ok(t, err)
	equals(t, []byte("DEF"), front.Data.Value())

	next := front.Next()
	equals(t, []byte("ABC"), next.Data.Value())

	err = ll.InsertBefore([]byte("GHI"), back)
	ok(t, err)

	// ABC's next, i.e. GHI - DEF was moved to front
	next = next.Next()

	back, err = ll.Back()
	ok(t, err)
	equals(t, []byte("JKL"), back.Data.Value())

	prev := back.Prev()
	equals(t, []byte("GHI"), prev.Data.Value())
	equals(t, string(next.Data.Value()), string(prev.Data.Value()))
}

func getfunc(a interface{}, b []byte) bool {
	return bytes.HasPrefix(b, a.([]byte))
}

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}
