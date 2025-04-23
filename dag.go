package main

import (
	"fmt"

	"github.com/google/uuid"
)

type Node struct {
	ID    string
	Name  string
	Edges []*Node
	Task  func() error
}

func NewNode(name string, task func() error) *Node {
	return &Node{
		ID:   uuid.New().String(),
		Name: name,
		Task: task,
	}
}

type Dag struct {
	Nodes    map[string]*Node
	inDegree map[string]int
}

func NewDag() *Dag {
	return &Dag{
		Nodes:    make(map[string]*Node),
		inDegree: make(map[string]int),
	}
}

func (d *Dag) AddNode(n *Node) error {
	if _, ok := d.Nodes[n.ID]; ok {
		return fmt.Errorf("node already exist")
	}

	d.Nodes[n.ID] = n
	d.inDegree[n.ID] = 0
	return nil
}

func (d *Dag) AddDependency(f, t *Node) error {
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

func (d *Dag) Run() error {
	queue := make([]*Node, 0)

	for k, v := range d.inDegree {
		if v == 0 {
			queue = append(queue, d.Nodes[k])
		}
	}

	for len(queue) > 0 {
		v := queue[0]
		//pop
		queue = queue[1:]

		err := v.Task()
		if err != nil {
			panic(err)
		}

		for _, edge := range v.Edges {
			d.inDegree[edge.ID] -= 1

			if d.inDegree[edge.ID] == 0 {
				queue = append(queue, edge)
			}
		}

	}

	return nil
}
