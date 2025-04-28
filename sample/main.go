package main

import (
	"context"
	"fmt"
	"time"

	"github.com/LincolnG4/simple-dag/dag"
)

func main() {

	d := dag.NewDag(4)

	timeout := 20 * time.Second
	n1 := d.AddNode("task1", func() error {
		fmt.Println("Doing 1")
		defer fmt.Println("End 1")
		time.Sleep(10 * time.Second)
		return nil
	}, timeout)

	n2 := d.AddNode("task2", func() error {
		fmt.Println("Doing 2")
		defer fmt.Println("End 2")

		time.Sleep(3 * time.Second)
		return nil
	}, timeout)

	n3 := d.AddNode("task3", func() error {
		fmt.Println("Doing 3")
		defer fmt.Println("End 3")

		time.Sleep(2 * time.Second)
		return nil
	}, timeout)

	n4 := d.AddNode("task4", func() error {
		fmt.Println("Doing 4")
		defer fmt.Println("End 4")

		time.Sleep(2 * time.Second)
		return nil
	}, timeout)

	n5 := d.AddNode("task5", func() error {
		fmt.Println("Doing 5")
		defer fmt.Println("End 5")

		time.Sleep(4 * time.Second)
		return nil
	}, timeout)

	d.AddDependency(n1, n2)
	d.AddDependency(n3, n2)
	d.AddDependency(n4, n5)

	// N1 --- N2
	// N3 --/
	// N4 -- N5

	ctx, cancelFunc := context.WithCancel(context.Background())
	go func() {
		time.Sleep(2 * time.Second)
		cancelFunc()
	}()
	err := d.Run(ctx)
	if err != nil {
		panic(err)
	}
}
