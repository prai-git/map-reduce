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
	//fOutPut, err := os.Create("output.txt")
	//f, err := os.Create("output.txt")

	defer f.Close()

	//w := bufio.NewWriter(f)

	fInput.TextFile(
		"input.txt", 2,
	).Map(func(line string, out chan flow.KeyValue) {

		array := strings.Split(line, "|")
		marks, _ := strconv.Atoi(array[2])

		out <- flow.KeyValue{Key: array[1], Value: marks}

	}).ReduceByKey(func(x int, y int) int {
		return (x + y) / 2
	}).Map(func(subject string, marks int) {
		if marks > 50 {
			str = fmt.Sprintf("%d %s", marks, subject)
			writeIntoFile(str)
		}
	})

}

func writeIntoFile(str string) {

	if f == nil {
		f, _ = os.Create("output.txt")
	}
	w := bufio.NewWriter(f)
	fmt.Printf(str)
	fmt.Fprintf(w, "%s\n", str)
	w.Flush()
}

func main() {
	flag.Parse()
	flow.Ready()

	fInput.Run()

}
