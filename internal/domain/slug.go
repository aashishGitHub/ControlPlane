package domain

import "regexp"

// slugPattern is the brief's exact rule: 3-28 chars total, lowercase, k8s-namespace
// compatible, must start with a letter. Broken down:
//   ^[a-z]              first char: a letter
//   [-a-z0-9]{1,26}     middle: 1-26 chars of letters/digits/hyphen
//   [a-z0-9]$           last char: letter or digit (no trailing hyphen)
// Total length is therefore 1 + (1..26) + 1 = 3..28, matching the brief exactly.
var slugPattern = regexp.MustCompile(`^[a-z][-a-z0-9]{1,26}[a-z0-9]$`)

// ValidSlug reports whether s satisfies the brief's slug rule. Kept as a plain
// function (not a Slug type wrapping a string) because the only two callers are the
// HTTP validation step and this package's own tests - a wrapper type would add
// ceremony without adding safety here.
func ValidSlug(s string) bool {
	return slugPattern.MatchString(s)
}
