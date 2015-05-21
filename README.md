Simple Bolt
============

WORK IN PROGRESS!

[![Build Status](https://travis-ci.org/xyproto/simplebolt.svg?branch=master)](https://travis-ci.org/xyproto/simplebolt)
[![GoDoc](https://godoc.org/github.com/xyproto/simplebolt?status.svg)](http://godoc.org/github.com/xyproto/simplebolt)

A way to use Bolt that is similar to [simpleredis](https://github.com/xyproto/simpleredis).


Online API Documentation
------------------------

[godoc.org](http://godoc.org/github.com/xyproto/simplebolt)


Features and limitations
------------------------

* Supports simple use of lists, hashmaps, sets and key/values
* Deals mainly with strings
* Uses the [redigo](https://github.com/garyburd/redigo) package


Example usage
-------------

~~~go
package main

import (
	"log"
	"fmt"

	"github.com/xyproto/simplebolt"
)

func main() {
	db := simplebolt.New("bolt.db")
	defer db.Close()

	kv := simplebolt.NewKeyValue(db, "fruit")
	if err := kv.Set("banana", "yes"); err != nil {
		log.Println("Could not set a key+value.")
		log.Fatalln(err)
	}

	val, err := kv.Get("banana")
	if err != nil {
		log.Println("Could not get value.")
		log.Fatalln(err)
	}
	fmt.Println("Got it:", val)

}
~~~

Version, license and author
---------------------------

* Version: 1.0
* License: MIT
* Author: Alexander F Rødseth

