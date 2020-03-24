package main

import (
	"fmt"
	"log"
	"io/ioutil"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/xyproto/simplebolt"
	"github.com/xyproto/simplebolt/linkedlist"
	pb "github.com/xyproto/simplebolt/example/linkedlist/currencypb"
)

type cryptoCurrency struct {
	cc *pb.Currency
}

func (data *cryptoCurrency) MustMarshal() []byte {
	b, err := proto.Marshal(data.cc)
	if err != nil {
		log.Fatalf("Could not marshal. %v\n", err)
	}
	return b
}

func (data *cryptoCurrency) MustUnmarshal(from []byte) {
	err := proto.Unmarshal(from, data.cc)
	if err != nil {
		log.Fatalf("Could not unmarshal. %v\n", err)
	}
}

func (data cryptoCurrency) GetName() string {
	return data.cc.GetName()
}

var cryptoCurrencies = []*cryptoCurrency{
	&cryptoCurrency{
		cc: &pb.Currency{
			Name:             "BTC",
			HighestPriceDate: "December 17, 2017",
			HighestPrice:     "19,891.0 USD",
		},
	}, &cryptoCurrency{
		cc: &pb.Currency{
			Name:             "ETH",
			HighestPriceDate: "January 13, 2018",
			HighestPrice:     "1,448.18 USD",
		},
	}, &cryptoCurrency{
		cc: &pb.Currency{
			Name:             "XRP",
			HighestPriceDate: "January 07, 2018",
			HighestPrice:     "3.40 USD",
		},
	},
}

var output = &cryptoCurrency{
	cc: &pb.Currency{},
}

func equals(a interface{}, b []byte) bool {
	data := &cryptoCurrency{
		cc: &pb.Currency{},
	}
	val := a.(string)
	data.MustUnmarshal(b)
	return data.cc.Name == val
}

func main() {
	// Create and open database
	ll, db := setUp()
	defer tearDown(db)

	var err error
	// Insert data at the end of the list
	for _, data := range cryptoCurrencies {
		fmt.Printf("PushBack data: %v\n", data.cc.Name)
		err = ll.PushBack(data.MustMarshal())
		if err != nil {
			log.Fatalf("Could not push back data. %v\n", err)
		}
	}
	list(ll)

	fmt.Println("\nInsert LTC before back")
	item, err := ll.Back()
	if err != nil {
		log.Fatalf("Could not get last item. %v\n", err)
	}
	ltc := &cryptoCurrency {
		cc: &pb.Currency{
			Name:             "LTC",
			HighestPriceDate: "December 18, 2017",
			HighestPrice:     "360.66 USD",
		},
	}
	err = ll.InsertBefore(ltc.MustMarshal(), item)
	if err != nil {
		log.Fatalf("Could not insert before back. %v\n", err)
	}

	list(ll)

	fmt.Println("\nGet ETH")
	item, err = ll.GetFunc("ETH", equals)
	if err != nil {
		log.Fatalf("Could not get ETH. %v\n", err)
	}
	output.MustUnmarshal(item.Data.Value())
	fmt.Printf("%v\n", output.cc.Name)

	err = ll.MoveToFront(item)
	if err != nil {
		log.Fatalf("Could not move ETH to front. %v\n", err)
	}
	fmt.Println("\nMoved ETH to front")
	item, err = ll.Front()
	if err != nil {
		log.Fatalf("Could not get front item. %v\n", err)
	}
	output.MustUnmarshal(item.Data.Value())
	fmt.Printf("Front: %v\n", output.cc.Name)

	list(ll)
}

func setUp() (*linkedlist.LinkedList, *simplebolt.Database) {
    // Retrieve a temporary path.
    f, err := ioutil.TempFile("", "")
    if err != nil {
        log.Fatalf("Could not create temp file. %v", err)
    }
    path := f.Name()
    f.Close()
    os.Remove(path)
    // Open the database.
    db, err := simplebolt.New(path)
    if err != nil {
        log.Fatalf("Could not open new database. %v", err)
    }
    ll, err := linkedlist.New(db, "Currencies")
    if err != nil {
        log.Fatalf("Could not create new list. %v", err)
    }
    return ll, db
}

func tearDown(db *simplebolt.Database) {
    defer os.Remove(db.Path())
    db.Close()
}

func list(ll *linkedlist.LinkedList) {
	// Iterate forwards
	forwards(ll)

	// Iterate backwards
	backwards(ll)
}

func backwards(ll *linkedlist.LinkedList) {
	item, err := ll.Back()
	if err != nil {
		log.Fatalf("Could not get last item. %v\n", err)
	}

	fmt.Println("\nBackward iteration")

	for item != nil {
		output.MustUnmarshal(item.Data.Value())
		name := output.cc.Name
		if item.Prev() != nil {
			output.MustUnmarshal(item.Prev().Data.Value())
			fmt.Printf("Prev: %v\t", output.cc.Name)
		} else {
			fmt.Printf("\t\t")
		}
		fmt.Printf("Name: %v\t", name)
		if item.Next() != nil {
			output.MustUnmarshal(item.Next().Data.Value())
			fmt.Printf("Next: %v\n", output.cc.Name)
		} else {
			fmt.Println()
		}
		item = item.Prev()
	}
}

func forwards(ll *linkedlist.LinkedList) {
	item, err := ll.Front()
	if err != nil {
		log.Fatalf("Could not get front item. %v\n", err)
	}

	fmt.Println("\nForward iteration")

	for item != nil {
		output.MustUnmarshal(item.Data.Value())
		name := output.cc.Name
		if item.Prev() != nil {
			output.MustUnmarshal(item.Prev().Data.Value())
			fmt.Printf("Prev: %v\t", output.cc.Name)
		} else {
			fmt.Printf("\t\t")
		}
		fmt.Printf("Name: %v\t", name)
		if item.Next() != nil {
			output.MustUnmarshal(item.Next().Data.Value())
			fmt.Printf("Next: %v\n", output.cc.Name)
		} else {
			fmt.Println()
		}
		item = item.Next()
	}
}