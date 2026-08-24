// Package shard splits tasks into parallel shards and tracks completion.
package shard

import (
	"fmt"
	"sort"
)

// Split divides a workload into count shards.
func Split(total int, count int) ([]int, error) {
	if total <= 0 {
		return nil, fmt.Errorf("shard: total must be positive")
	}
	if count <= 0 {
		return nil, fmt.Errorf("shard: count must be positive")
	}
	if count > total {
		count = total
	}
	base := total / count
	remainder := total % count
	sizes := make([]int, 0, count)
	for i := 0; i < count; i++ {
		size := base
		if i < remainder {
			size++
		}
		sizes = append(sizes, size)
	}
	sort.Ints(sizes)
	return sizes, nil
}
