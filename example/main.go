package main

import (
	"log"

	"github.com/xyproto/simplebolt"
)

func main() {
	// Check if the bolt service is up
	if err := simplebolt.TestConnection(); err != nil {
		log.Fatalln("Could not connect to Bolt. Is the service up and running?")
	}

	// Use instead for testing if a different host/port is up.
	// simplebolt.TestConnectionHost("localhost:6379")

	// Create a connection pool, connect to the given bolt server
	pool := simplebolt.NewConnectionPool()

	// Use this for connecting to a different bolt host/port
	// pool := simplebolt.NewConnectionPoolHost("localhost:6379")

	// For connecting to a different bolt host/port, with a password
	// pool := simplebolt.NewConnectionPoolHost("password@bolthost:6379")

	// Close the connection pool right after this function returns
	defer pool.Close()

	// Create a list named "greetings"
	list := simplebolt.NewList(pool, "greetings")

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
