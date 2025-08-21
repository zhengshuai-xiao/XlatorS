package internal

type StringSet struct {
	m map[string]struct{}
}

func NewStringSet() *StringSet {
	return &StringSet{
		m: make(map[string]struct{}),
	}
}

func (s *StringSet) Add(item string) {
	s.m[item] = struct{}{}
}

func (s *StringSet) Remove(item string) {
	delete(s.m, item)
}

func (s *StringSet) Contains(item string) bool {
	_, exists := s.m[item]
	return exists
}

func (s *StringSet) Len() int {
	return len(s.m)
}

func (s *StringSet) Elements() []string {
	elements := make([]string, 0, len(s.m))
	for item := range s.m {
		elements = append(elements, item)
	}
	return elements
}

type Int64Set struct {
	m map[int64]struct{}
}

func NewInt64Set() *Int64Set {
	return &Int64Set{
		m: make(map[int64]struct{}),
	}
}

func (s *Int64Set) Add(item int64) {
	s.m[item] = struct{}{}
}

func (s *Int64Set) Remove(item int64) {
	delete(s.m, item)
}

func (s *Int64Set) Contains(item int64) bool {
	_, exists := s.m[item]
	return exists
}

func (s *Int64Set) Len() int {
	return len(s.m)
}

func (s *Int64Set) Elements() []int64 {
	elements := make([]int64, 0, len(s.m))
	for item := range s.m {
		elements = append(elements, item)
		logger.Tracef("Elements:%d", item)
	}
	return elements
}
