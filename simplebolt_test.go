package simplebolt

import (
	sqldb "github.com/xyproto/db"
	"testing"
)

func TestList(t *testing.T) {
	const (
		listname = "abc123_test_test_test_123abc"
		testdata = "123abc"
	)
	db := New("/tmp/__test_simplebolt1.db")
	defer db.Close()
	list := NewList(db, listname)
	if err := list.Add(testdata); err != nil {
		t.Errorf("Error, could not add item to list! %s", err.Error())
	}
	items, err := list.GetAll()
	if len(items) != 1 {
		t.Errorf("Error, wrong list length! %v", len(items))
	}
	if (len(items) > 0) && (items[0] != testdata) {
		t.Errorf("Error, wrong list contents! %v", items)
	}
	err = list.Remove()
	if err != nil {
		t.Errorf("Error, could not remove list! %s", err.Error())
	}
}

func TestRemove(t *testing.T) {
	const (
		kvname    = "abc123_test_test_test_123abc"
		testkey   = "sdsdf234234"
		testvalue = "asdfasdf1234"
	)
	db := New("/tmp/__test_simplebolt2.db")
	defer db.Close()
	kv := NewKeyValue(db, kvname)
	if err := kv.Set(testkey, testvalue); err != nil {
		t.Errorf("Error, could not set key and value! %s", err.Error())
	}
	if val, err := kv.Get(testkey); err != nil {
		t.Errorf("Error, could not get key! %s", err.Error())
	} else if val != testvalue {
		t.Errorf("Error, wrong value! %s != %s", val, testvalue)
	}
	kv.Remove()
	if _, err := kv.Get(testkey); err == nil {
		t.Errorf("Error, could get key! %s", err.Error())
	}
}

func TestInc(t *testing.T) {
	const (
		kvname     = "kv_234_test_test_test"
		testkey    = "key_234_test_test_test"
		testvalue0 = "9"
		testvalue1 = "10"
		testvalue2 = "1"
	)
	db := New("/tmp/__test_simplebolt3.db")
	defer db.Close()
	kv := NewKeyValue(db, kvname)
	if err := kv.Set(testkey, testvalue0); err != nil {
		t.Errorf("Error, could not set key and value! %s", err.Error())
	}
	if val, err := kv.Get(testkey); err != nil {
		t.Errorf("Error, could not get key! %s", err.Error())
	} else if val != testvalue0 {
		t.Errorf("Error, wrong value! %s != %s", val, testvalue0)
	}
	incval, err := kv.Inc(testkey)
	if err != nil {
		t.Errorf("Error, could not INCR key! %s", err.Error())
	}
	if val, err := kv.Get(testkey); err != nil {
		t.Errorf("Error, could not get key! %s", err.Error())
	} else if val != testvalue1 {
		t.Errorf("Error, wrong value! %s != %s", val, testvalue1)
	} else if incval != testvalue1 {
		t.Errorf("Error, wrong inc value! %s != %s", incval, testvalue1)
	}
	kv.Remove()
	if _, err := kv.Get(testkey); err == nil {
		t.Errorf("Error, could get key! %s", err.Error())
	}
	// Creates "0" and increases the value with 1
	kv.Inc(testkey)
	if val, err := kv.Get(testkey); err != nil {
		t.Errorf("Error, could not get key! %s", err.Error())
	} else if val != testvalue2 {
		t.Errorf("Error, wrong value! %s != %s", val, testvalue2)
	}
	kv.Remove()
	if _, err := kv.Get(testkey); err == nil {
		t.Errorf("Error, could get key! %s", err.Error())
	}
}

func TestTwoFields(t *testing.T) {
	test, test23, ok := twoFields("test1@test2@test3", "@")
	if ok && ((test != "test1") || (test23 != "test2@test3")) {
		t.Error("Error in twoFields functions")
	}
}

func TestVarious(t *testing.T) {
	db := New("/tmp/__test_simplebolt4.db")
	defer db.Close()

	kv := NewKeyValue(db, "fruit")
	if err := kv.Set("banana", "yes"); err != nil {
		t.Error("Could not set a key+value:", err)
	}

	val, err := kv.Get("banana")
	if err != nil {
		t.Error("Could not get value:", err)
	}

	kv.Set("banana", "2")
	kv.Inc("banana")
	_, err = kv.Get("banana")
	if err != nil {
		t.Error(err)
	}

	kv.Inc("fnu")
	_, err = kv.Get("fnu")
	if err != nil {
		t.Error(err)
	}

	val, err = kv.Get("doesnotexist")
	//fmt.Println("does not exist", val, err)

	kv.Remove()

	l := NewList(db, "fruit")

	l.Add("kiwi")
	l.Add("banana")
	l.Add("pear")
	l.Add("apple")

	if _, err := l.GetAll(); err != nil {
		t.Error(err)
	}

	last, err := l.GetLast()
	if err != nil {
		t.Error(err)
	}
	if last != "apple" {
		t.Error("last one should be apple")
	}

	lastN, err := l.GetLastN(3)
	if err != nil {
		t.Error(err)
	}
	if lastN[0] != "banana" {
		t.Error("banana is wrong")
	}

	l.Remove()

	s := NewSet(db, "numbers")
	s.Add("9")
	s.Add("7")
	s.Add("2")
	s.Add("2")
	s.Add("2")
	s.Add("7")
	s.Add("8")
	_, err = s.GetAll()
	if err != nil {
		t.Error(err)
	}
	s.Remove()

	val, err = kv.Inc("counter")
	if (val != "1") || (err != nil) {
		t.Error("counter should be 1 but is", val)
	}
	kv.Remove()

	h := NewHashMap(db, "counter")
	h.Set("bob", "password", "hunter1")
	h.Set("bob", "email", "bob@zombo.com")
	h.GetAll()

	_, err = h.Has("bob", "password")
	if err != nil {
		t.Error(err)
	}

	_, err = h.Exists("bob")
	if err != nil {
		t.Error(err)
	}

	h.Remove()

	_, err = h.Has("bob", "password")
	if err == nil {
		t.Error("not supposed to exist")
	}

	_, err = h.Exists("bob")
	if err == nil {
		t.Error("not supposed to exist")
	}
}

func TestInterface(t *testing.T) {

	db := New("/tmp/__test_simplebolt5.db")
	defer db.Close()

	// Check that the database qualifies for the IHost interface
	var _ sqldb.IHost = db
}
