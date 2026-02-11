package pg

import "time"

func calculateBatch(
	numEvents int,
	minBatch int,
	maxBatch int,
) int {
	if maxBatch == 0 {
		maxBatch = 100
	}

	var batch int
	switch {
	case numEvents == 0:
		batch = minBatch
	case numEvents >= maxBatch:
		batch = maxBatch
	default:
		batch = numEvents * 2
	}

	if batch < minBatch {
		batch = minBatch
	}
	if batch > maxBatch {
		batch = maxBatch
	}

	return batch
}

func calculateSleep(
	numEvents int,
	maxBatch int,
	lastSleep time.Duration,
	minSleep time.Duration,
	maxSleep time.Duration,
) time.Duration {

	var sleep time.Duration

	switch {
	case numEvents == 0:
		if lastSleep == 0 {
			sleep = minSleep
		} else {
			sleep = lastSleep * 2
		}

	case numEvents >= maxBatch:
		sleep = 0

	default:
		fill := float64(numEvents) / float64(maxBatch)

		switch {
		case fill >= 0.75:
			sleep = 0
		case fill >= 0.5:
			sleep = minSleep
		case fill >= 0.25:
			sleep = minSleep * 2
		default:
			sleep = minSleep * 4
		}
	}

	if sleep < minSleep {
		sleep = minSleep
	}
	if sleep > maxSleep {
		sleep = maxSleep
	}

	return sleep
}
