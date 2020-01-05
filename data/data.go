// Package data defines the common set of methods among containers
// to get and set the values of their underlying data.

package data

// StoredData is the set of methods that provides access to every container.
type StoredData interface {
	Value() []byte
	Update([]byte) error
	Remove() error
}
