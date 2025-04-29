package dag

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func generateValidDag() *Dag {
	d := NewDag(30*time.Second, 2)
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

func Test_DagTimeoutRun(t *testing.T) {
	d := NewDag(100*time.Millisecond, 1) // DAG timeout

	timeout := 50 * time.Second // Node timeout (not critical here)

	n1 := d.AddNode("task1", func() error {
		fmt.Println("Doing 1")
		defer fmt.Println("End 1")
		time.Sleep(200 * time.Millisecond) // longer than DAG timeout
		return nil
	}, timeout)

	n2 := d.AddNode("task2", func() error {
		fmt.Println("Doing 2")
		defer fmt.Println("End 2")
		time.Sleep(100 * time.Millisecond)
		return nil
	}, timeout)

	n3 := d.AddNode("task3", func() error {
		fmt.Println("Doing 3")
		defer fmt.Println("End 3")
		time.Sleep(100 * time.Millisecond)
		return nil
	}, timeout)

	d.AddDependency(n1, n2)
	d.AddDependency(n3, n2)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	start := time.Now()
	err := d.Run(ctx)
	elapsed := time.Since(start)

	fmt.Println("Run finished with error:", err)

	if err == nil {
		t.Fatal("expected timeout error but got nil")
	}

	// Check if error is DAG timeout
	if !errors.Is(err, ErrDAGCancelled) && !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("expected DAG timeout error, got: %v", err)
	}

	// Optional: check if timeout really happened early
	if elapsed > 500*time.Millisecond {
		t.Fatalf("DAG took too long to timeout: %v", elapsed)
	}
}

func Test_TaskTimeoutRun(t *testing.T) {
	d := NewDag(100*time.Second, 1) // DAG timeout

	timeout := 50 * time.Second // Node timeout (not critical here)

	n1 := d.AddNode("task1", func() error {
		fmt.Println("Doing 1")
		defer fmt.Println("End 1")
		time.Sleep(200 * time.Millisecond) // longer than DAG timeout
		return nil
	}, 100*time.Millisecond)

	n2 := d.AddNode("task2", func() error {
		fmt.Println("Doing 2")
		defer fmt.Println("End 2")
		time.Sleep(100 * time.Millisecond)
		return nil
	}, timeout)

	n3 := d.AddNode("task3", func() error {
		fmt.Println("Doing 3")
		defer fmt.Println("End 3")
		time.Sleep(100 * time.Millisecond)
		return nil
	}, timeout)

	d.AddDependency(n1, n2)
	d.AddDependency(n3, n2)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	err := d.Run(ctx)

	fmt.Println("Run finished with error:", err)

	if err == nil {
		t.Fatal("expected timeout error but got nil")
	}

	// Check if error is DAG timeout
	if !errors.Is(err, ErrTaskCancelled) && !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("expected DAG timeout error, got: %v", err)
	}

}
