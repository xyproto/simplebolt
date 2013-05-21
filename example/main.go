package main

import (
	"fmt"
	"github.com/xyproto/simpleredis"
)

func main() {
	// Create a connection pool, connects to the local redis server
	pool := simpleredis.NewConnectionPool()

	// Close the connection pool when this function returns
	defer pool.Close()

	// Create a list named "greetings"
	list := simpleredis.NewList(pool, "greetings")

	// Add "hello" to the list
	err := list.Add("hello")
	if err != nil {
		panic("Could not add an item to list! Is Redis up and running?")
	}

	// Get the last item in the list
	item, err := list.GetLast()
	if err != nil {
		panic("Could not fetch the last item from the list!")
	}
	fmt.Println("The value of the stored item is:", item)

	// Remove the list
	err = list.Remove()
	if err != nil {
		panic("Could not remove the list!")
	}
}
