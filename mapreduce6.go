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
	str    string
	f      *os.File
)

func init() {
	saveCountToFile(50)
}

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

func writeIntoFile(count int) {

	f, _ := os.Create("output.txt")
	w := bufio.NewWriter(f)
	fmt.Fprintf(w, "%d\n", count)
	w.Flush()
}

func main() {
	flag.Parse()
	flow.Ready()

	fInput.Run()
}
