package mapreduce

import (
	"os"
	"encoding/json"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	var keys []string                   // store all keys in this partition
	var kvs = make(map[string][]string) // store all key-value pairs from nMap intermediate files

	// read nMap intermediate files from map workers
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		intermediateFile, err := os.Open(fileName)
		if err != nil {
			debug("open immediate file %s failed", fileName)
			continue
		}

		var kv KeyValue
		decoder := json.NewDecoder(intermediateFile)
		err = decoder.Decode(&kv)
		for err == nil {
			if _, ok := kvs[kv.Key]; !ok {
				keys = append(keys, kv.Key)
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)

			// decode repeatedly until an error
			err = decoder.Decode(&kv)
		}
	}

	// keys in one partition are processed in increasing key order
	sort.Strings(keys)

	out, err := os.Create(outFile)
	if err != nil {
		debug("create output file %s failed", outFile)
		return
	}

	encoder := json.NewEncoder(out)
	for _, key := range keys {
		if err := encoder.Encode(KeyValue{key, reduceF(key, kvs[key])}); err != nil {
			debug("write [key: %s] to file %s failed", key, outFile)
		}
	}
	out.Close()
}
