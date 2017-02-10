package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
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
	saveCountToFile(averageScore)
}

// initialies data transformation using map-reduce-filter
func saveCountToFile(avScore int) {
	fInput.TextFile(
		"input.txt", 2,
	).Map(func(line string, out chan flow.KeyValue) {

		array := strings.Split(line, "|")
		marks, _ := strconv.Atoi(array[2])

		out <- flow.KeyValue{Key: array[1], Value: marks}

	}).ReduceByKey(func(x int, y int) int {
		return (x + y) / 2
	}).Filter(func(subject string, marks int) bool {
		return marks >= avScore
	}).Map(func(key string, marks int) int {
		return 1
	}).Reduce(func(x int, y int) int {
		return x + y
	}).Map(func(x int) {
		println("count:", x)
		writeIntoFile(x)
	})
}

/*
Write count into File
*/
func writeIntoFile(count int) {

	f, _ := os.Create("output.txt")
	w := bufio.NewWriter(f)
	fmt.Fprintf(w, "%d\n", count)
	w.Flush()
}

func main() {
	// We need to add this magic line to compute it in cluster.
	flag.Parse()
	flow.Ready()
	// start computing
	fInput.Run()
}
