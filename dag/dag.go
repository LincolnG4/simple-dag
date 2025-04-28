package dag

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidDAG       = errors.New("invalid DAG: contains cycles")
	ErrTaskCancelled    = errors.New("task cancelled")
	ErrDAGCancelled     = errors.New("DAG cancelled")
	ErrTaskFailed       = errors.New("task failed")
	ErrDependencyFailed = errors.New("dependency failed")
)

type Node struct {
	ID      string
	Name    string
	Edges   []*Node
	Task    func() error
	Timeout time.Duration
}

func (n *Node) RunTask() error {
	if n.Task == nil {
		return nil
	}

	return n.Task()
}

type Dag struct {
	Nodes      map[string]*Node
	inDegree   map[string]int
	workerPool chan struct{} // Semaphore for worker pool
	Timeout    time.Duration
}

func NewDag(timeout time.Duration) *Dag {
	return &Dag{
		Nodes:      make(map[string]*Node),
		inDegree:   make(map[string]int),
		workerPool: make(chan struct{}, 4),
		Timeout:    timeout,
	}
}

func (d *Dag) AddNode(name string, task func() error, timeout time.Duration) *Node {
	n := &Node{
		ID:      uuid.New().String(),
		Name:    name,
		Task:    task,
		Timeout: timeout,
	}

	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	n.Timeout = timeout

	d.Nodes[n.ID] = n
	d.inDegree[n.ID] = 0

	return n
}

func (d *Dag) IsValid() bool {
	inDegreeCopy := d.copyInDegree()

	queue := make([]*Node, 0)
	for id, degree := range inDegreeCopy {
		if degree == 0 {
			queue = append(queue, d.Nodes[id])
		}
	}

	count := 0
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		count++

		for _, neighbor := range node.Edges {
			inDegreeCopy[neighbor.ID]--
			if inDegreeCopy[neighbor.ID] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	return count == len(d.Nodes)
}

func (d *Dag) AddDependency(f, t *Node) error {
	if f == t {
		return fmt.Errorf("source can't be equal target")
	}

	_, ok := d.Nodes[f.ID]
	if !ok {
		return fmt.Errorf("dependency source task '%s' not found", f.ID)
	}
	_, ok = d.Nodes[t.ID]
	if !ok {
		return fmt.Errorf("dependency target task '%s' not found", t.ID)
	}

	f.Edges = append(f.Edges, t)
	d.inDegree[t.ID] += 1

	return nil
}

func (d *Dag) run(errChannel chan error) {
	// make a copy of indegree
	inDegree := d.copyInDegree()

	// task queue
	queue := make(chan *Node, len(d.Nodes))

	// get 0-level in degree
	for i, degree := range inDegree {
		if degree == 0 {
			queue <- d.Nodes[i]
		}
	}

	var mu sync.Mutex
	var wg sync.WaitGroup

	// put nodes to the queue
	count := 0
	for count < len(d.Nodes) {
		// pop node
		node := <-queue

		wg.Add(1)
		count++
		go func(node *Node) {
			defer wg.Done()

			err := node.RunTask()
			if err != nil {
				fmt.Println(err)
				errChannel <- err
			}

			mu.Lock()
			// put edge nodes into queue
			for _, n := range node.Edges {
				inDegree[n.ID] -= 1
				if inDegree[n.ID] == 0 {
					queue <- n
				}
			}
			mu.Unlock()
		}(node)
	}

	wg.Wait()
	errChannel <- nil
}

func (d *Dag) Run(ctx context.Context) error {
	if !d.IsValid() {
		return ErrInvalidDAG
	}

	ctx, cancel := context.WithTimeout(ctx, d.Timeout)
	defer cancel()

	errChannel := make(chan error, 1)

	go d.run(errChannel)

	select {
	case err := <-errChannel:
		return err
	case <-ctx.Done():
		return fmt.Errorf("%v:%v", ErrDAGCancelled, ctx.Err())
	}
}

func (d *Dag) copyInDegree() map[string]int {
	m := make(map[string]int, len(d.inDegree))
	for k, v := range d.inDegree {
		m[k] = v
	}
	return m
}
