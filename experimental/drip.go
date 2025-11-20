package main

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

func main() {

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	c := make(chan arrow.Record)

	go func() {

		builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
		builder.Field(0).(*array.StringBuilder).AppendValues([]string{"a1", "a2", "a3"}, nil)

		time.Sleep(time.Second * 5)
		c <- builder.NewRecord()

		builder.Field(0).(*array.StringBuilder).AppendValues([]string{"a4", "a5", "a6", "a7", "a8", "a9"}, nil)

		time.Sleep(time.Second * 4)
		c <- builder.NewRecord()

		builder.Field(0).(*array.StringBuilder).AppendValues([]string{"a10", "a11", "a12", "a13", "a14", "a15"}, nil)

		time.Sleep(time.Second * 4)
		c <- builder.NewRecord()

		close(c)
	}()

	for x := range drip(c) {
		fmt.Println(x)
	}
}

func drip(c chan arrow.Record) chan arrow.Record {

	c3 := make(chan arrow.Record, 1)

	go func() {

		c2 := make(chan arrow.Record, 1)

		var processing arrow.Record

		running := true

		go func() {
			for x := range c {
				c2 <- x
			}
			running = false
		}()

		processIndex := 0
		processChunk := 2
		for running {
			select {
			case x, ok := <-c2:
				if ok {
					if processing != nil {
						c3 <- processing.NewSlice(int64(processIndex), int64(processing.NumRows()))
					}
					processIndex = 0
					processing = x
				} else {
					running = false
				}
			default:
				if processing != nil && processIndex < int(processing.NumRows()) {

					if processIndex+processChunk >= int(processing.NumRows()) {
						c3 <- processing.NewSlice(int64(processIndex), int64(processing.NumRows()))
						processIndex = int(processing.NumRows())
					} else {
						c3 <- processing.NewSlice(int64(processIndex), int64(processIndex+processChunk))
						processIndex += processChunk
					}
				}
			}

			time.Sleep(time.Second * 1)
		}

		c3 <- processing.NewSlice(int64(processIndex), int64(processing.NumRows()))
		processIndex = 0

		processing = <-c2

		c3 <- processing.NewSlice(int64(processIndex), int64(processing.NumRows()))

		close(c3)

	}()

	return c3
}
