package usercache

// classifyDepthWithProfile 根据给定的深度和背压配置文件，分类背压等级
func classifyDepthWithProfile(depth int64, profile BackpressureDelayProfile, softLimit, hardLimit int) BackpressureLevel {
	if depth <= 0 {
		return BackpressureNone
	}
	if meetsBackpressureBucket(depth, profile.Severe) {
		return BackpressureSevere
	}
	if meetsBackpressureBucket(depth, profile.Elevated) {
		return BackpressureElevated
	}
	if hardLimit > 0 && depth >= int64(hardLimit) {
		return BackpressureSevere
	}
	if softLimit > 0 && depth >= int64(softLimit) {
		return BackpressureElevated
	}
	return BackpressureNone
}

// meetsBackpressureBucket 检查给定深度是否满足任一延迟桶的条件
func meetsBackpressureBucket(depth int64, buckets []BackpressureDelayBucket) bool {
	if depth <= 0 {
		return false
	}
	for _, bucket := range buckets {
		if bucket.Depth <= 0 {
			continue
		}
		if depth >= int64(bucket.Depth) {
			return true
		}
	}
	return false
}
