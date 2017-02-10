package main

import (
	"strconv"
	"strings"

	"github.com/chrislusf/glow/flow"
)

func main() {
	//wc()
	saveCountToFile(5)
}

func wc() {
	flow.New().TextFile(
		"input.txt", 3,
	).Filter(func(line string) bool {
		return !strings.HasPrefix(line, "#")
	}).Map(func(line string, ch chan string) {
		for _, token := range strings.Split(line, "|") {
			ch <- token
		}
	}).Map(func(key string) int {
		return 1
	}).Reduce(func(x int, y int) int {
		return x + y
	}).Map(func(x int) {
		println("count:", x)
	}).Run()

}

func saveCountToFile(avScore int) {
	flow.New().TextFile(
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
	}).Run()
}
