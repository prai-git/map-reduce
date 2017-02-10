package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/chrislusf/glow/driver"
	"github.com/chrislusf/glow/flow"
)

type Record struct {
	marks   int
	subject string
	id      int
}

var (
	f = flow.New()
)

func init() {
	questions := f.TextFile(
		"input.txt", 2,
	).Map(func(line string, ch chan Record) {
		var p Record
		array := strings.Split(line, "|")
		p.id, _ = strconv.Atoi(array[0])
		p.subject = array[1]
		p.marks, _ = strconv.Atoi(array[2])

		ch <- p
	}).Map(func(src Record) (p Record) {
		p.id = src.id
		p.subject = src.subject
		p.marks = src.marks
		return
	})

	questions.Map(func(p Record, out chan flow.KeyValue) {
		out <- flow.KeyValue{Key: p.subject, Value: p.marks}
	}).ReduceByKey(func(x int, y int) int {
		return (x + y) / 2
	}).Map(func(subject string, marks int) {
		if marks > 50 {
			fmt.Printf("%d %s\n", marks, subject)
		}
	})
}

func main() {
	flag.Parse()
	flow.Ready()

	f.Run()

}
