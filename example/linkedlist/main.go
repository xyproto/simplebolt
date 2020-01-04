package main

import (
	"fmt"
	"log"

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
			HighestPrice:     "19,891.00 USD",
		},
	}, &cryptoCurrency{
		cc: &pb.Currency{
			Name:             "ETH",
			HighestPriceDate: "January 13, 2018",
			HighestPrice:     "1,448.18 USD",
		},
	}, &cryptoCurrency{
		cc: &pb.Currency{
			Name:             "LTC",
			HighestPriceDate: "December 18, 2017",
			HighestPrice:     "360.66 USD",
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

func main() {
	// Create and open database
	db, err := simplebolt.New("data/base.db")
	if err != nil {
		log.Fatalf("Could not create new database. %v\n", err)
	}
	fmt.Println("database successfully opened")
	defer db.Close()
	// Create or get linked list
	ll, err := linkedlist.New(db, "Currencies")
	if err != nil {
		log.Fatalf("Could not create new linked list. %v\n", err)
	}
	// Insert data at the end of the list
	for _, data := range cryptoCurrencies {
		fmt.Printf("PushBack data: %+v\n", data.cc)
		err = ll.PushBack(data.MustMarshal())
		if err != nil {
			log.Fatalf("Could not push back data. %v\n", err)
		}
	}
	// Iterate forwards
	item, err := ll.Front()
	if err != nil {
		log.Fatalf("Could not get first item. %v\n", err)
	}
	fmt.Println("\nForward iteration")
	for item != nil {
		output.MustUnmarshal(item.Data.Value())
		fmt.Printf("%+v\n", output.cc)
		item = item.Next()
	}
	// Iterate backwards
	item, err = ll.Back()
	if err != nil {
		log.Fatalf("Could not get last item. %v\n", err)
	}
	fmt.Println("\nBackward iteration")
	for item != nil {
		output.MustUnmarshal(item.Data.Value())
		fmt.Printf("%+v\n", output.cc)
		item = item.Prev()
	}
}
