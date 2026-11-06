package change

// Step changes c in place. Returning false drops the change.
//
// Any function with this signature can be passed wherever steps are accepted,
// so a new step is usable as soon as it is written.
type Step func(c *Change) bool

// Run runs steps in order and stops at the first one that drops the change.
func Run(c *Change, steps ...Step) bool {
	for _, step := range steps {
		if !step(c) {
			return false
		}
	}
	return true
}
