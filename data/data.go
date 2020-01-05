// Package data defines the common set of methods among containers
// to get and set the values of their underlying data.

package data

// StoredData is the set of methods that provides access to the
// element's underlying data in every data structure.
type StoredData interface {
	// Value returns the current value of the element at which the item refers
	// to.
	Value() []byte

	// Update resets the value of the element at which the item refers to
	// with newData.
	//
	// Returns "Empty data" error if newData is nil.
	Update(newData []byte) error

	// Remove deletes from Bolt the element at which the item data refers to.
	Remove() error
}
