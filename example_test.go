package simpleblob_test

import (
	"context"
	"fmt"

	"github.com/PowerDNS/simpleblob"

	// Register the memory backend plugin
	_ "github.com/PowerDNS/simpleblob/backends/memory"
)

func Example() {
	// Do not forget the:
	//     import _ "github.com/PowerDNS/simpleblob/backends/memory"
	ctx := context.Background()
	storage, err := simpleblob.GetBackend(ctx, "memory", map[string]interface{}{})
	check(err)
	err = storage.Store(ctx, "example.txt", []byte("hello"))
	check(err)
	data, err := storage.Load(ctx, "example.txt")
	check(err)
	fmt.Println("data:", string(data))
	list, err := storage.List(ctx, "")
	check(err)
	for _, entry := range list {
		fmt.Println("list:", entry.Name, entry.Size)
	}
	// Output:
	// data: hello
	// list: example.txt 5
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
