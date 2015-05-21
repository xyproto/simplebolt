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
