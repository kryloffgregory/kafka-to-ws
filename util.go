package main

import "math"

func getOffsetFromTimestamp(timestamp int64) int64 {
	res:= int64(math.Ceil(float64(timestamp-1546300800)/60))
	if res < 0 {
		return 0
	}
	return res
}
