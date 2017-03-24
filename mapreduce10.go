package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	_ "github.com/chrislusf/glow/driver"
	"github.com/chrislusf/glow/flow"
)

var (
	fInput = flow.New()
)

const averageScore int = 50

/*
This method called before main().
*/
func init() {
	saveCountToFile("cities.csv", "IN_AP_PUATR")
}

// initialies data transformation using map-reduce-filter
func saveCountToFile(filePath string, filterStr string) {
	fInput.TextFile(
		filePath, 2,
	).Map(func(line string, out chan flow.KeyValue) {

		array := strings.Split(line, ",")
		//marks, _ := strconv.Atoi(array[2])
		key := array[2] + "_" + array[1] + "_" + array[0]

		out <- flow.KeyValue{Key: key, Value: line}

	}).Filter(func(key string, marks string) bool {
		return key == filterStr
	}).Map(func(key1 string, marks string) string {
		fmt.Println(key1)
		writeIntoFile(key1)
		return key1
	})
}

/*
Write count into File
*/
func writeIntoFile(count string) {

	f, _ := os.Create("output.txt")
	w := bufio.NewWriter(f)
	fmt.Fprintf(w, "%s\n", count)
	w.Flush()
}

func main() {
	// We need to add this magic line to compute it in cluster.
	flag.Parse()
	flow.Ready()
	// start computing
	fInput.Run()
}
