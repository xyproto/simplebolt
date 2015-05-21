Simple Bolt
============

[![Build Status](https://travis-ci.org/xyproto/simplebolt.svg?branch=master)](https://travis-ci.org/xyproto/simplebolt)
[![GoDoc](https://godoc.org/github.com/xyproto/simplebolt?status.svg)](http://godoc.org/github.com/xyproto/simplebolt)

Simple way to use Bolt. Similar to [simpleredis](https://github.com/xyproto/simpleredis).


Online API Documentation
------------------------

[godoc.org](http://godoc.org/github.com/xyproto/simplebolt)


Features and limitations
------------------------

* Supports simple use of lists, hashmaps, sets and key/values
* Deals mainly with strings


Example usage
-------------

~~~go
package main

import (
	"log"

	"github.com/xyproto/simplebolt"
)

func main() {
	// New bolt database
	db := simplebolt.New("bolt.db")
	defer db.Close()

	// Create a list named "greetings"
	list := simplebolt.NewList(db, "greetings")

	// Add "hello" to the list, check if there are errors
	if list.Add("hello") != nil {
		log.Fatalln("Could not add an item to list!")
	}

	// Get the last item of the list
	if item, err := list.GetLast(); err != nil {
		log.Fatalln("Could not fetch the last item from the list!")
	} else {
		log.Println("The value of the stored item is:", item)
	}

	// Remove the list
	if list.Remove() != nil {
		log.Fatalln("Could not remove the list!")
	}
}
~~~

Version, license and author
---------------------------

* Version: 1.0
* License: MIT
* Author: Alexander F Rødseth

