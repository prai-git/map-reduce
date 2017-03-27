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

type Permission struct {
	parent   *Permission
	incluse []string
	excluse []string
}

type Region struct {
	parent   *Permission
	incluse []string
	excluse []string
}

const recordSet *Dataset;
const keyValueMap [string]string = [];

var (
	fInput = flow.New()
)

/*
This method called before main().
*/
func init() {
	creatMap("cities.csv") // , "IN_AP_PUATR")
}

func hasKey():void
{
	
}

func hasPermission(key, permission Permission) bool{
	permissionBool bool
	if(permission.parent != null)
	{
		permissionBool =  hasPermission(key, permission.parent)
		if(permissionBool == false)
		{
			return false
		}
	}
	
	recordSet.Filter(func(key string, marks string) bool {
		return key == filterStr
	}).Map(func(key1 string, marks string) string {
		fmt.Println(key1)
		writeIntoFile(key1)
		return key1
	})
}


// initialies data transformation using map-reduce-filter
func creatMap(filePath string) *Dataset {
	recordSet = fInput.TextFile(
		filePath, 2,
	).Map(func(line string, out chan flow.KeyValue) {

		array := strings.Split(line, ",")
		//marks, _ := strconv.Atoi(array[2])
		key := array[3] + "_" + array[4] + "_" + array[5]
		out <- flow.KeyValue{Key: key, Value: line}
	})
	
	return &recordSet;
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
