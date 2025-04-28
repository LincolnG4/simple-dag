package dag

import (
	"context"
	"fmt"
	"testing"
)

func generateValidDag() *Dag {
	d := NewDag(3)
	n1 := d.AddNode("task1", func() error {
		fmt.Println("Doing 1")
		defer fmt.Println("End 1")

		return nil
	}, 10)

	n2 := d.AddNode("task2", func() error {
		fmt.Println("Doing 2")
		defer fmt.Println("End 2")

		return nil
	}, 10)

	n3 := d.AddNode("task3", func() error {
		fmt.Println("Doing 3")
		defer fmt.Println("End 3")

		return nil
	}, 10)

	d.AddDependency(n1, n2)
	d.AddDependency(n3, n2)

	return d
}

func Test_InvalidDag(t *testing.T) {
	d := generateValidDag()

	if !d.IsValid() {
		t.Error("IsValid() returned false, but should return true")
	}

	n1 := d.AddNode("task1", func() error {
		fmt.Println("Doing 1")
		defer fmt.Println("End 1")

		return nil
	}, 10)

	n2 := d.AddNode("task2", func() error {
		fmt.Println("Doing 2")
		defer fmt.Println("End 2")

		return nil
	}, 10)

	d.AddDependency(n1, n2)
	d.AddDependency(n2, n1)

	if d.IsValid() {
		t.Error("IsValid() returned true, but should be false")
	}
}

func Test_DagRun(t *testing.T) {
	d := generateValidDag()

	ctx := context.Background()
	err := d.Run(ctx)
	if err != nil {
		t.Errorf("Run() of valid dag returned %s", err)

	}
	n1 := d.AddNode("task1", func() error {
		fmt.Println("Doing 1")
		defer fmt.Println("End 1")

		return nil
	}, 10)

	n2 := d.AddNode("task2", func() error {
		fmt.Println("Doing 2")
		defer fmt.Println("End 2")

		return nil
	}, 10)

	d.AddDependency(n1, n2)
	d.AddDependency(n2, n1)

	err = d.Run(ctx)
	if err == nil {
		t.Error("Run() of invalid dag should fail")
	}
}
