package hq

import "time"

func pathToHops(path string) (hops int) {
	// For each L in the path, add 1 hop
	for _, c := range path {
		if c == 'L' {
			hops++
		}
	}

	return hops
}

func hopsToPath(hops int) (path string) {
	// For each hop, add an L to the path
	for i := 0; i < hops; i++ {
		path += "L"
	}

	return path
}

// resetTimer safely resets the timer to the specified duration.
func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
}
