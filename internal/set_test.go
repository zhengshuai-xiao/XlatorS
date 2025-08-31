package internal

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringSet(t *testing.T) {
	s := NewStringSet()

	// Test Add and Contains
	s.Add("apple")
	s.Add("banana")
	s.Add("apple") // Add duplicate

	assert.True(t, s.Contains("apple"))
	assert.True(t, s.Contains("banana"))
	assert.False(t, s.Contains("cherry"))

	// Test Len
	assert.Equal(t, 2, s.Len())

	// Test Remove
	s.Remove("apple")
	assert.False(t, s.Contains("apple"))
	assert.Equal(t, 1, s.Len())

	// Test Elements
	s.Add("cherry")
	elements := s.Elements()
	sort.Strings(elements)
	assert.Equal(t, []string{"banana", "cherry"}, elements)
}

func TestUInt64Set(t *testing.T) {
	s := NewUInt64Set()

	s.Add(10)
	s.Add(20)
	s.Add(10) // Add duplicate

	assert.True(t, s.Contains(10))
	assert.True(t, s.Contains(20))
	assert.False(t, s.Contains(30))
	assert.Equal(t, 2, s.Len())

	s.Remove(10)
	assert.False(t, s.Contains(10))
	assert.Equal(t, 1, s.Len())
}
