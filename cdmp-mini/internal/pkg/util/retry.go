package util

import "time"

func RetryWithBackoff(maxRetry int, shouldRetry func(error) bool, fn func() (interface{}, error)) (interface{}, error) {
	var lastErr error
	for i := 0; i < maxRetry; i++ {
		result, err := fn()
		if err == nil {
			return result, nil
		}
		lastErr = err
		if !shouldRetry(err) {
			break
		}
		time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
	}
	return nil, lastErr
}
