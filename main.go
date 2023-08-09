package main

import (
	"flag"
	"fmt"
	"timestream_hands_on/timestream"
)

func main() {

	mode := flag.String("mode", "write", "write or query")

	flag.Parse()

	switch *mode {
	case "write":
		timestream.Write()
	case "query":
		timestream.Query()
	default:
		fmt.Println("invalid mode")
	}
}
