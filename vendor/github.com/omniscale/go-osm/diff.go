package osm

// A Diff contains a change operation on a single OSM element.
type Diff struct {
	// Create specifies whether the element was created.
	Create bool
	// Modify specifies whether the element was modified.
	Modify bool
	// Delete specifies whether the element was deleted.
	Delete bool
	// Node points to the actual node, if a node was changed.
	Node *Node
	// Way points to the actual way, if a way was changed.
	Way *Way
	// Rel points to the actual relation, if a relation was changed.
	Rel *Relation
}
