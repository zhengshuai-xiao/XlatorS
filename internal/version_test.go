package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseVersion(t *testing.T) {
	testCases := []struct {
		name        string
		versionStr  string
		expected    *Semver
		expectError bool
	}{
		{"Valid Full Version", "1.2.3-alpha+build123", &Semver{major: 1, minor: 2, patch: 3, preRelease: "alpha"}, false},
		{"Valid No Pre-release", "1.2.3+build123", &Semver{major: 1, minor: 2, patch: 3, preRelease: ""}, false},
		{"Valid No Build", "1.2.3-beta", &Semver{major: 1, minor: 2, patch: 3, preRelease: "beta"}, false},
		{"Valid Major Minor", "1.2", &Semver{major: 1, minor: 2, patch: 0, preRelease: ""}, false},
		{"Valid Major Only", "1", &Semver{major: 1, minor: 0, patch: 0, preRelease: ""}, false},
		{"Invalid String", "abc", nil, true},
		{"Invalid Too Many Parts", "1.2.3.4", nil, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parsed := Parse(tc.versionStr)
			if tc.expectError {
				assert.Nil(t, parsed)
			} else {
				assert.NotNil(t, parsed)
				// We only compare the parsed parts, not the build string which is ignored
				assert.Equal(t, tc.expected.major, parsed.major)
				assert.Equal(t, tc.expected.minor, parsed.minor)
				assert.Equal(t, tc.expected.patch, parsed.patch)
				assert.Equal(t, tc.expected.preRelease, parsed.preRelease)
			}
		})
	}
}

func TestCompareVersions(t *testing.T) {
	v1_0_0 := &Semver{major: 1, minor: 0, patch: 0}
	v1_1_0 := &Semver{major: 1, minor: 1, patch: 0}
	v1_1_1 := &Semver{major: 1, minor: 1, patch: 1}
	v2_0_0 := &Semver{major: 2, minor: 0, patch: 0}
	v1_0_0_alpha := &Semver{major: 1, minor: 0, patch: 0, preRelease: "alpha"}
	v1_0_0_beta := &Semver{major: 1, minor: 0, patch: 0, preRelease: "beta"}

	testCases := []struct {
		name     string
		v1, v2   *Semver
		expected int
	}{
		{"v1 < v2 (major)", v1_0_0, v2_0_0, -1},
		{"v2 > v1 (major)", v2_0_0, v1_0_0, 1},
		{"v1 < v2 (minor)", v1_0_0, v1_1_0, -1},
		{"v1 < v2 (patch)", v1_1_0, v1_1_1, -1},
		{"v1 == v2", v1_1_1, v1_1_1, 0},
		{"pre-release alpha < beta", v1_0_0_alpha, v1_0_0_beta, -1},
		{"release > pre-release", v1_0_0, v1_0_0_alpha, 1},
		{"pre-release < release", v1_0_0_alpha, v1_0_0, -1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := CompareVersions(tc.v1, tc.v2)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, res)
		})
	}
}
