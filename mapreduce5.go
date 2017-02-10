package main

import (
	"flag"
	"strconv"
	"strings"

	_ "github.com/chrislusf/glow/driver"
	"github.com/chrislusf/glow/flow"
)

var (
	fInput = flow.New()
	//f      *os.File
)

/* map-reduce program to get Average */

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
	}).Map(func(key string) int {
		// println("map:", key)
		return 1
	}).Reduce(func(x int, y int) int {
		// println("reduce:", x+y)
		return x + y
	}).Map(func(x int) {
		println("count:", x)
	}).Run()
}

func main() {

	// We need to add this magic line to initialize Glow.
	flag.Parse()
	flow.Ready()
	// To run the task in distributed mod.
	saveCountToFile(50)
}
