package integration

import "testing"

// The exact-state oracle must preserve JSON types while comparing numeric
// spellings mathematically. Rounding or converting numbers to strings would
// otherwise let a corrupted projection satisfy a qualification gate.
func TestOnlineJSONOracleTypes(t *testing.T) {
	cases := []struct {
		name, source, target string
		equal                bool
	}{
		{"number-is-not-string", `{"n":1}`, `{"n":"1"}`, false},
		{"fraction-is-not-string", `{"n":0.5}`, `{"n":"1/2"}`, false},
		{"nested-array-types", `{"values":[1,{"n":0.5}]}`, `{"values":["1",{"n":"1/2"}]}`, false},
		{"large-number-is-not-string", `{"n":9007199254740993}`, `{"n":"9007199254740993"}`, false},
		{"large-number-precision", `{"n":9007199254740993}`, `{"n":9007199254740992}`, false},
		{"numeric-spellings", `{"n":1}`, `{"n":1.0}`, true},
		{"exponent-spellings", `{"n":1e3}`, `{"n":1000.00}`, true},
		{"nested-numeric-spellings", `{"values":[1,{"n":0.5}]}`, `{"values":[1.00,{"n":5e-1}]}`, true},
		{"literal-strings", `{"n":"1/2"}`, `{"n":"1/2"}`, true},
		{"null-is-not-string", `{"n":null}`, `{"n":"null"}`, false},
		{"trailing-input", `{"n":1} {"n":2}`, `{"n":1}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := losslessJSONEqual(tc.source, tc.target); got != tc.equal {
				t.Errorf("source/target equality = %v, want %v", got, tc.equal)
			}
			if got := losslessJSONEqual(tc.target, tc.source); got != tc.equal {
				t.Errorf("target/source equality = %v, want %v", got, tc.equal)
			}
		})
	}
}
