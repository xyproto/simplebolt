package simpleredis

import (
	"testing"
)

// TODO: Add tests for all the datatypes and, ideally, all the available functions

var pool *ConnectionPool

func TestLocalConnection(t *testing.T) {
	err := TestConnectionSimple()
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestConnectionPool(t *testing.T) {
	pool = NewConnectionPool()
}

func TestList(t *testing.T) {
	const (
		listname = "abc123_test_test_test_123abc"
		testdata = "123abc"
	)
	list := NewList(pool, listname)
	list.SelectDatabase(1)
	err := list.Add(testdata)
	if err != nil {
		t.Errorf("Error, could not add item to list! %s", err)
	}
	items, err := list.GetAll()
	if len(items) != 1 {
		t.Errorf("Error, wrong list length! %s", err)
	}
	if (len(items) > 0) && (items[0] != testdata) {
		t.Errorf("Error, wrong list contents! %s", err)
	}
	err = list.Remove()
	if err != nil {
		t.Errorf("Error, could not remove list! %s", err)
	}
}
