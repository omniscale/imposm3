/*
Package mapping provides implements mapping and convertion between OSM elements and database tables, rows and columns.

The core logic of Imposm is accesible with the Mapping struct.
A Mapping creates filters and matchers based on mapping configuration (.yaml or .json file).

Filters are for initial filtering (during -read). They remove all tags that are not needed.

Matchers map OSM elements to zero or more destination tables. Each Match results can convert an OSM element
to a row with all mapped column values.
The matching is dependend on the element type (node, way, relation), the element tags and the destination
table type (point, linestring, polygon, relation, relation_member).
*/
package mapping
