package main

import (
	"fmt"
	"time"

	"github.com/LincolnG4/simple-dag/dag"
)

func main() {

	d := dag.NewDag()

	n1 := dag.NewNode("task1", func() error {
		fmt.Println("Doing 1")
		time.Sleep(1 * time.Second)
		return nil
	})

	n2 := dag.NewNode("task2", func() error {
		fmt.Println("Doing 2")
		time.Sleep(1 * time.Second)
		return nil
	})

	n3 := dag.NewNode("task3", func() error {
		fmt.Println("Doing 3")
		time.Sleep(1 * time.Second)
		return nil
	})

	err := d.AddNode(n1)
	if err != nil {
		panic(err)
	}

	err = d.AddNode(n2)
	if err != nil {
		panic(err)
	}

	err = d.AddNode(n3)
	if err != nil {
		panic(err)
	}

	d.AddDependency(n1, n2)
	d.AddDependency(n1, n3)

	valid := d.IsValid()
	if !valid {
		panic("NOT VALID DAG")
	}
	fmt.Println(valid)

	err = d.Run()
	if err != nil {
		panic(err)
	}
}
