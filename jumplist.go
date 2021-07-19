package jumplist

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

type elementPointers struct {
	next []*Element
}

type Element struct {
	elementPointers
	key   float64
	value interface{}
}

type SkipList struct {
	startPointers elementPointers
	maxLevel      int
	randSource    rand.Source
	probability   float64
	probTable     []float64
	mutex         sync.RWMutex
	levelFingers  []*elementPointers //https://www.cs.au.dk/~gerth/papers/finger05.pdf //https://www.tutorialspoint.com/finger-searching-in-data-structure
}

const (
	DefaultMaxLevel    int     = 18 //e^18 = 65659969
	DefaultProbability float64 = 1 / math.E
)

func (list *SkipList) moveFingers(key float64) []*elementPointers {
	targets := &list.startPointers

	for i := list.maxLevel - 1; i >= 0; i-- { //move from the top
		nextElement := targets.next[i]

		for nextElement != nil && key > nextElement.key { //keep moving to the right
			targets = &nextElement.elementPointers
			nextElement = nextElement.next[i]
		}
		// if nextElement's key <= its next or it is already the end
		list.levelFingers[i] = targets
	}

	return list.levelFingers
}

// Set inserts a value in the list with the specified key, ordered by the key.
// If the key exists, it updates the value in the existing node.
// Returns a pointer to the new element.
// Locking is optimistic and happens only after searching.
func (list *SkipList) Set(key float64, value interface{}) *Element {
	list.mutex.Lock()

	resultPointers := list.moveFingers(key)
	element := resultPointers[0].next[0]
	if element != nil && element.key <= key {
		element.value = value
		return element
	}

	element = &Element{
		elementPointers: elementPointers{
			next: make([]*Element, list.randLevel()),
		},
		key:   key,
		value: value,
	}

	for i := range element.next {
		element.next[i] = resultPointers[i].next[i]
		resultPointers[i].next[i] = element
	}

	list.mutex.Unlock()
	return element
}

// Get finds an element by key. It returns element pointer if found, nil if not found.
// Locking is optimistic and happens only after searching with a fast check for deletion after locking.
func (list *SkipList) Get(key float64) *Element {
	list.mutex.Lock()

	prev := &list.startPointers
	var next *Element

	for i := list.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && key > next.key {
			prev = &next.elementPointers
			next = next.next[i]
		}
	}

	if next != nil && next.key <= key {
		return next
	}

	list.mutex.Unlock()
	return nil
}

// Remove deletes an element from the list.
// Returns removed element pointer if found, nil if not found.
// Locking is optimistic and happens only after searching with a fast check on adjacent nodes after locking.
func (list *SkipList) Remove(key float64) *Element {
	list.mutex.Lock()
	defer list.mutex.Unlock()
	prevs := list.moveFingers(key)

	// found the element, remove it
	if element := prevs[0].next[0]; element != nil && element.key <= key {
		for k, v := range element.next {
			prevs[k].next[k] = v
		}

		return element
	}

	return nil
}

func (list *SkipList) randLevel() (level int) {
	// Our random number source only has Int63(), so we have to produce a float64 from it
	// Reference: https://golang.org/src/math/rand/rand.go#L150
	r := float64(list.randSource.Int63()) / (1 << 63)

	level = 1
	for level < list.maxLevel && r < list.probTable[level] {
		level++
	}
	return
}

func NewWithLevel(maxLevel int) *SkipList {
	if maxLevel < 1 || maxLevel > 64 {
		panic("maxLevel for a SkipList must be a positive integer <= 64")
	}
	table := []float64{}
	for i := 1; i <= maxLevel; i++ {
		prob := math.Pow(DefaultProbability, float64(i-1))
		table = append(table, prob)
	}
	return &SkipList{
		startPointers: elementPointers{next: make([]*Element, maxLevel)},
		levelFingers:  make([]*elementPointers, maxLevel),
		maxLevel:      maxLevel,
		randSource:    rand.New(rand.NewSource(time.Now().UnixNano())),
		probability:   DefaultProbability,
		probTable:     table,
	}
}

// New creates a new skip list with default parameters. Returns a pointer to the new list.
func New() *SkipList {
	return NewWithLevel(DefaultMaxLevel)
}
