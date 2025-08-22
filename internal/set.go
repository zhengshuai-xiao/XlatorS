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

type UInt64Set struct {
	m map[uint64]struct{}
}

func NewUInt64Set() *UInt64Set {
	return &UInt64Set{
		m: make(map[uint64]struct{}),
	}
}

func (s *UInt64Set) Add(item uint64) {
	s.m[item] = struct{}{}
}

func (s *UInt64Set) Remove(item uint64) {
	delete(s.m, item)
}

func (s *UInt64Set) Contains(item uint64) bool {
	_, exists := s.m[item]
	return exists
}

func (s *UInt64Set) Len() int {
	return len(s.m)
}

func (s *UInt64Set) Elements() []uint64 {
	elements := make([]uint64, 0, len(s.m))
	for item := range s.m {
		elements = append(elements, item)
		logger.Tracef("Elements:%d", item)
	}
	return elements
}
