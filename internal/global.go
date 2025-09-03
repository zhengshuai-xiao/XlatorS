package internal

import "time"

var (
	GlobalOperationTimeout       = NewDynamicTimeout(10*time.Minute, 5*time.Minute) // default timeout for general ops
	GlobalDeleteOperationTimeout = NewDynamicTimeout(5*time.Minute, 1*time.Minute)  // default time for delete ops
	GlobalGetLockConfigTimeout   = NewDynamicTimeout(5*time.Second, 1*time.Second)  // default timeout for get config lock

)
