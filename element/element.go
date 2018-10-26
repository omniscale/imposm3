package element

import (
	"sort"
)

type IDRefs struct {
	ID   int64
	Refs []int64
}

func (idRefs *IDRefs) Add(ref int64) {
	i := sort.Search(len(idRefs.Refs), func(i int) bool {
		return idRefs.Refs[i] >= ref
	})
	if i < len(idRefs.Refs) && idRefs.Refs[i] >= ref {
		if idRefs.Refs[i] > ref {
			idRefs.Refs = append(idRefs.Refs, 0)
			copy(idRefs.Refs[i+1:], idRefs.Refs[i:])
			idRefs.Refs[i] = ref
		} // else already inserted
	} else {
		idRefs.Refs = append(idRefs.Refs, ref)
	}
}

func (idRefs *IDRefs) Delete(ref int64) {
	i := sort.Search(len(idRefs.Refs), func(i int) bool {
		return idRefs.Refs[i] >= ref
	})
	if i < len(idRefs.Refs) && idRefs.Refs[i] == ref {
		idRefs.Refs = append(idRefs.Refs[:i], idRefs.Refs[i+1:]...)
	}
}

// RelIDOffset is a constant we subtract from relation IDs
// to avoid conflicts with way and node IDs.
// Nodes, ways and relations have separate ID spaces in OSM, but
// we need unique IDs for updating and removing elements in diff mode.
// In a normal diff import relation IDs are negated to distinguish them
// from way IDs, because ways and relations can both be imported in the
// same polygon table.
// Nodes are only imported together with ways and relations in single table
// imports (see `type_mappings`). In this case we negate the way and
// relation IDs and aditionaly subtract RelIDOffset from the relation IDs.
// Ways will go from -0 to -100,000,000,000,000,000, relations from
// -100,000,000,000,000,000 down wards.
const RelIDOffset = -1e17
