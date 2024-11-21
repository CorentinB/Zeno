package reactor

// GetStateTable returns a slice of all the seeds UUIDs as string in the state table.
func GetStateTable() []string {
	keys := []string{}
	globalReactor.stateTable.Range(func(key, _ interface{}) bool {
		keys = append(keys, key.(string))
		return true
	})
	return keys
}
