package domain

import "testing"

func TestValidSlug(t *testing.T) {
	cases := []struct {
		slug string
		want bool
	}{
		// valid
		{"abc", true},                          // minimum length (3)
		{"a222222222222222222222222227", true}, // exactly 28 chars (1+26+1)
		{"acme", true},
		{"acme-prod", true},
		{"a1b2c3", true},

		// too short
		{"", false},
		{"a", false},
		{"ab", false}, // 2 chars: fails - needs >=3

		// too long
		{"a2222222222222222222222222228", false}, // 29 chars

		// must start with a letter
		{"1abc", false},
		{"-abc", false},

		// must not end with a hyphen
		{"abc-", false},

		// disallowed characters
		{"ABC", false},     // uppercase
		{"abc_def", false}, // underscore
		{"abc.def", false}, // dot
		{"abc def", false}, // space
	}

	for _, c := range cases {
		if got := ValidSlug(c.slug); got != c.want {
			t.Errorf("ValidSlug(%q) = %v, want %v", c.slug, got, c.want)
		}
	}
}
