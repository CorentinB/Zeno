package stats

/////////////////////////
//     URLsCrawled     //
/////////////////////////

// URLsCrawledIncr increments the URLsCrawled counter by 1.
func URLsCrawledIncr() { globalStats.URLsCrawled.incr(1) }

// URLsCrawledGet returns the current value of the URLsCrawled counter.
func URLsCrawledGet() uint64 { return globalStats.URLsCrawled.get() }

// URLsCrawledReset resets the URLsCrawled counter to 0.
func URLsCrawledReset() { globalStats.URLsCrawled.reset() }

/////////////////////////
//    SeedsFinished    //
/////////////////////////

// SeedsFinishedIncr increments the SeedsFinished counter by 1.
func SeedsFinishedIncr() { globalStats.SeedsFinished.incr(1) }

// SeedsFinishedGet returns the current value of the SeedsFinished counter.
func SeedsFinishedGet() uint64 { return globalStats.SeedsFinished.get() }

// SeedsFinishedReset resets the SeedsFinished counter to 0.
func SeedsFinishedReset() { globalStats.SeedsFinished.reset() }

//////////////////////////
// PreprocessorRoutines //
//////////////////////////

// PreprocessorRoutinesIncr increments the PreprocessorRoutines counter by 1.
func PreprocessorRoutinesIncr() { globalStats.PreprocessorRoutines.incr(1) }

// PreprocessorRoutinesDecr decrements the PreprocessorRoutines counter by 1.
func PreprocessorRoutinesDecr() { globalStats.PreprocessorRoutines.decr(1) }

// PreprocessorRoutinesGet returns the current value of the PreprocessorRoutines counter.
func PreprocessorRoutinesGet() uint64 { return globalStats.PreprocessorRoutines.get() }

// PreprocessorRoutinesReset resets the PreprocessorRoutines counter to 0.
func PreprocessorRoutinesReset() { globalStats.PreprocessorRoutines.reset() }

//////////////////////////
//  ArchiverRoutines    //
//////////////////////////

// ArchiverRoutinesIncr increments the ArchiverRoutines counter by 1.
func ArchiverRoutinesIncr() { globalStats.ArchiverRoutines.incr(1) }

// ArchiverRoutinesDecr decrements the ArchiverRoutines counter by 1.
func ArchiverRoutinesDecr() { globalStats.ArchiverRoutines.decr(1) }

// ArchiverRoutinesGet returns the current value of the ArchiverRoutines counter.
func ArchiverRoutinesGet() uint64 { return globalStats.ArchiverRoutines.get() }

// ArchiverRoutinesReset resets the ArchiverRoutines counter to 0.
func ArchiverRoutinesReset() { globalStats.ArchiverRoutines.reset() }

//////////////////////////
// PostprocessorRoutines //
//////////////////////////

// PostprocessorRoutinesIncr increments the PostprocessorRoutines counter by 1.
func PostprocessorRoutinesIncr() { globalStats.PostprocessorRoutines.incr(1) }

// PostprocessorRoutinesDecr decrements the PostprocessorRoutines counter by 1.
func PostprocessorRoutinesDecr() { globalStats.PostprocessorRoutines.decr(1) }

// PostprocessorRoutinesGet returns the current value of the PostprocessorRoutines counter.
func PostprocessorRoutinesGet() uint64 { return globalStats.PostprocessorRoutines.get() }

// PostprocessorRoutinesReset resets the PostprocessorRoutines counter to 0.
func PostprocessorRoutinesReset() { globalStats.PostprocessorRoutines.reset() }
