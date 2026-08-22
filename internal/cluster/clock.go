package cluster

import "time"

func leaseNow() int64 {
	return time.Now().UnixNano()
}
