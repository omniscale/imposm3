package binary

// Serialize tags to a array of interleaved key and value strings.
// Common tags like building=yes are serialized to a single unicode
// char to save a few bytes. Common keys to a single ASCII ctrl char.

// Common tags are encoded as a single unicode char from the Unicode
// Private Use Area http://en.wikipedia.org/wiki/Private_Use_(Unicode)
// between U+E000 and U+F8FF. They take three bytes in UTF-8 encoding.
//
// For example: building=yes will need 4 bytes instead of 13 bytes.
// (building=8 + yes=3 + 2x1 for string length, vs. 3+1)

// The most common tag kays with variable values (name, addr:street,
// etc.) are converted to a single ASCII control char (0x01-0x1f)

import (
	"unicode/utf8"

	"github.com/omniscale/imposm3/element"
)

type codepoint rune
type tag struct {
	Key   string
	Value string
}

var tagsToCodePoint = map[string]map[string]codepoint{}
var codePointToTag = map[codepoint]tag{}

var commonKeys = map[string]codepoint{}
var codePointToCommonKey = map[uint8]string{}
var nextKeyCodePoint = codepoint(1)
var maxKeyCodePoint = codepoint(31)

const minCodePoint = codepoint('\uE000')
const maxCodePoint = codepoint('\uF8FF')

var nextCodePoint = codepoint('\uE000')

const escapeRune = '\ufffd' // unicode replacement char

func addTagCodePoint(key, value string) {
	if nextCodePoint > maxCodePoint {
		panic("all codepoints used!")
	}
	valMap, ok := tagsToCodePoint[key]
	if !ok {
		tagsToCodePoint[key] = map[string]codepoint{value: nextCodePoint}
	} else {
		if _, ok := valMap[value]; ok {
			panic("duplicate entry for tag codepoints: " + key + " " + value)
		}
		valMap[value] = nextCodePoint
	}

	codePointToTag[nextCodePoint] = tag{key, value}
	nextCodePoint += 1
}

func addCommonKey(key string) {
	if nextKeyCodePoint > maxKeyCodePoint {
		panic("all codepoints used!")
	}
	commonKeys[key] = nextKeyCodePoint
	codePointToCommonKey[uint8(nextKeyCodePoint)] = key
	nextKeyCodePoint += 1
}

func tagsFromArray(arr []string) element.Tags {
	if len(arr) == 0 {
		return element.Tags{}
	}
	result := make(element.Tags)
	for i := 0; i < len(arr); i += 1 {
		if r, size := utf8.DecodeRuneInString(arr[i]); size >= 3 {
			if r == escapeRune {
				// remove escape rune
				result[arr[i][size:]] = arr[i+1]
				i++
				continue
			} else if codepoint(r) >= minCodePoint &&
				codepoint(r) < nextCodePoint {
				tag, ok := codePointToTag[codepoint(r)]
				if !ok {
					panic("missing tag for codepoint")
				}
				result[tag.Key] = tag.Value
				continue
			}
		} else if len(arr[i]) > 0 && arr[i][0] < 32 {
			result[codePointToCommonKey[arr[i][0]]] = arr[i][1:]
			continue
		}
		if len(arr) <= i+1 {
			// notify users affected by #112
			// TODO remove check in the future to avoid misleading message
			// if a similar issue shows up
			panic("Internal cache corrupt, see: https://github.com/omniscale/imposm3/issues/122")
		}
		result[arr[i]] = arr[i+1]
		i++
	}
	return result
}

func tagsAsArray(tags element.Tags) []string {
	if len(tags) == 0 {
		return nil
	}
	result := make([]string, 0, 2*len(tags))
	for key, val := range tags {
		result = appendTag(result, key, val)
	}
	return result
}

func appendTag(arr []string, key, val string) []string {
	if valMap, ok := tagsToCodePoint[key]; ok {
		if codePoint, ok := valMap[val]; ok {
			return append(arr, string(codePoint))
		}
	}
	if codepoint, ok := commonKeys[key]; ok {
		return append(arr, string(codepoint)+val)
	}
	// escape first char/rune if it is a commonKey/tagCodePoint
	if len(key) > 0 && key[0] < 32 {
		key = string(escapeRune) + key
	} else if r, size := utf8.DecodeRuneInString(key); size >= 3 &&
		((codepoint(r) >= minCodePoint &&
			codepoint(r) <= maxCodePoint) ||
			(r == escapeRune)) {
		key = string(escapeRune) + key
	}
	return append(arr, key, val)
}

func init() {
	//
	// DO NOT EDIT, REMOVE, REORDER ANY OF THE FOLLOWING LINES!
	//

	// most common keys with variable values
	// there are only 31 code points for common keys
	addCommonKey("name")
	addCommonKey("addr:street")
	addCommonKey("addr:place")
	addCommonKey("addr:city")
	addCommonKey("addr:postcode")
	addCommonKey("addr:housenumber")

	// most used tags for ways
	//
	addTagCodePoint("building", "yes")
	addTagCodePoint("highway", "residential")
	addTagCodePoint("highway", "service")
	addTagCodePoint("wall", "no")
	addTagCodePoint("highway", "unclassified")
	addTagCodePoint("waterway", "stream")
	addTagCodePoint("highway", "track")
	addTagCodePoint("natural", "water")
	addTagCodePoint("oneway", "yes")
	addTagCodePoint("highway", "footway")
	addTagCodePoint("highway", "tertiary")
	addTagCodePoint("access", "private")
	addTagCodePoint("highway", "path")
	addTagCodePoint("highway", "secondary")
	addTagCodePoint("landuse", "forest")
	addTagCodePoint("building", "house")
	addTagCodePoint("bridge", "yes")
	addTagCodePoint("surface", "asphalt")
	addTagCodePoint("natural", "wood")
	addTagCodePoint("foot", "yes")
	addTagCodePoint("landuse", "residential")
	addTagCodePoint("surface", "paved")
	addTagCodePoint("highway", "primary")
	addTagCodePoint("surface", "unpaved")
	addTagCodePoint("landuse", "grass")
	addTagCodePoint("building", "residential")
	addTagCodePoint("service", "parking_aisle")
	addTagCodePoint("oneway", "no")
	addTagCodePoint("railway", "rail")
	addTagCodePoint("bicycle", "yes")
	addTagCodePoint("service", "driveway")
	addTagCodePoint("amenity", "parking")
	addTagCodePoint("area", "yes")
	addTagCodePoint("barrier", "fence")
	addTagCodePoint("tracktype", "grade2")
	addTagCodePoint("natural", "coastline")
	addTagCodePoint("tracktype", "grade3")
	addTagCodePoint("intermittent", "yes")
	addTagCodePoint("landuse", "farmland")
	addTagCodePoint("building", "hut")
	addTagCodePoint("boundary", "administrative")
	addTagCodePoint("lit", "yes")
	addTagCodePoint("highway", "cycleway")
	addTagCodePoint("landuse", "meadow")
	addTagCodePoint("waterway", "river")
	addTagCodePoint("natural", "wetland")
	addTagCodePoint("highway", "trunk")
	addTagCodePoint("surface", "gravel")
	addTagCodePoint("tracktype", "grade1")
	addTagCodePoint("barrier", "wall")
	addTagCodePoint("building", "garage")
	addTagCodePoint("highway", "living_street")
	addTagCodePoint("highway", "motorway")
	addTagCodePoint("tracktype", "grade4")
	addTagCodePoint("landuse", "farm")
	addTagCodePoint("leisure", "pitch")
	addTagCodePoint("surface", "ground")
	addTagCodePoint("tunnel", "yes")
	addTagCodePoint("highway", "motorway_link")
	addTagCodePoint("bicycle", "no")
	addTagCodePoint("highway", "road")
	addTagCodePoint("natural", "scrub")
	addTagCodePoint("highway", "steps")
	addTagCodePoint("foot", "designated")
	addTagCodePoint("waterway", "ditch")
	addTagCodePoint("admin_level", "8")
	addTagCodePoint("tracktype", "grade5")
	addTagCodePoint("access", "yes")
	addTagCodePoint("building", "apartments")
	addTagCodePoint("leisure", "swimming_pool")
	addTagCodePoint("junction", "roundabout")
	addTagCodePoint("highway", "pedestrian")
	addTagCodePoint("barrier", "hedge")
	addTagCodePoint("bicycle", "designated")
	addTagCodePoint("leisure", "park")
	addTagCodePoint("service", "alley")
	addTagCodePoint("landuse", "farmyard")
	addTagCodePoint("building", "industrial")
	addTagCodePoint("waterway", "riverbank")
	addTagCodePoint("building", "roof")
	addTagCodePoint("surface", "dirt")
	addTagCodePoint("waterway", "drain")
	addTagCodePoint("surface", "grass")
	addTagCodePoint("amenity", "school")
	addTagCodePoint("power", "line")
	addTagCodePoint("landuse", "industrial")
	addTagCodePoint("landuse", "reservoir")
	addTagCodePoint("water", "intermittent")
	addTagCodePoint("highway", "trunk_link")
	addTagCodePoint("segregated", "no")
	addTagCodePoint("horse", "no")
	addTagCodePoint("wood", "deciduous")
	addTagCodePoint("highway", "primary_link")
	addTagCodePoint("foot", "no")
	addTagCodePoint("lit", "no")
	addTagCodePoint("surface", "concrete")
	addTagCodePoint("building", "garages")
	addTagCodePoint("amenity", "place_of_worship")
	addTagCodePoint("religion", "christian")
	addTagCodePoint("waterway", "canal")
	addTagCodePoint("landuse", "orchard")
	addTagCodePoint("surface", "paving_stones")
	addTagCodePoint("leisure", "garden")
	addTagCodePoint("service", "spur")
	addTagCodePoint("living_street", "yes")
	addTagCodePoint("access", "permissive")
	addTagCodePoint("sport", "soccer")
	addTagCodePoint("frequency", "0")
	addTagCodePoint("landuse", "cemetery")
	addTagCodePoint("wood", "mixed")
	addTagCodePoint("motorcar", "no")
	addTagCodePoint("access", "no")
	addTagCodePoint("man_made", "pier")
	addTagCodePoint("oneway", "-1")
	addTagCodePoint("sport", "tennis")
	addTagCodePoint("noexit", "yes")
	addTagCodePoint("service", "yard")
	addTagCodePoint("wood", "coniferous")
	addTagCodePoint("natural", "cliff")
	addTagCodePoint("leisure", "playground")
	addTagCodePoint("cycleway", "lane")
	addTagCodePoint("surface", "cobblestone")
	addTagCodePoint("landuse", "vineyard")
	addTagCodePoint("frequency", "16.7")
	//
	// most used tags for nodes
	//
	addTagCodePoint("power", "tower")
	addTagCodePoint("natural", "tree")
	addTagCodePoint("highway", "bus_stop")
	addTagCodePoint("power", "pole")
	addTagCodePoint("place", "locality")
	addTagCodePoint("highway", "turning_circle")
	addTagCodePoint("highway", "crossing")
	addTagCodePoint("place", "village")
	addTagCodePoint("place", "hamlet")
	addTagCodePoint("highway", "traffic_signals")
	addTagCodePoint("barrier", "gate")
	// addTagCodePoint("admin_level", "8")
	// addTagCodePoint("amenity", "place_of_worship")
	// addTagCodePoint("amenity", "school")
	// addTagCodePoint("religion", "christian")
	addTagCodePoint("amenity", "bench")
	addTagCodePoint("man_made", "survey_point")
	addTagCodePoint("amenity", "restaurant")
	// addTagCodePoint("amenity", "parking")
	addTagCodePoint("natural", "peak")
	addTagCodePoint("railway", "level_crossing")
	addTagCodePoint("type", "broad_leaved")
	// addTagCodePoint("building", "yes")
	// addTagCodePoint("foot", "yes")
	// addTagCodePoint("bicycle", "yes")
	addTagCodePoint("highway", "street_lamp")
	addTagCodePoint("tourism", "information")
	addTagCodePoint("wheelchair", "yes")
	addTagCodePoint("building", "entrance")
	addTagCodePoint("public_transport", "stop_position")
	addTagCodePoint("amenity", "fuel")
	// addTagCodePoint("noexit", "yes")
	addTagCodePoint("barrier", "bollard")
	addTagCodePoint("amenity", "post_box")
	addTagCodePoint("natural", "rock")
	// addTagCodePoint("landuse", "forest")
	addTagCodePoint("shelter", "yes")
	addTagCodePoint("emergency", "fire_hydrant")
	addTagCodePoint("public_transport", "platform")
	addTagCodePoint("amenity", "grave_yard")
	addTagCodePoint("shop", "convenience")
	addTagCodePoint("power", "generator")
	addTagCodePoint("shop", "supermarket")
	addTagCodePoint("amenity", "bank")
	addTagCodePoint("amenity", "fast_food")
	addTagCodePoint("amenity", "cafe")
	//
	// most used tags for rels
	//
	addTagCodePoint("type", "multipolygon")
	addTagCodePoint("type", "route")
	// addTagCodePoint("boundary", "administrative")
	addTagCodePoint("type", "restriction")
	addTagCodePoint("type", "boundary")
	addTagCodePoint("type", "site")
	// addTagCodePoint("admin_level", "8")
	addTagCodePoint("type", "associatedStreet")
	// addTagCodePoint("natural", "water")
}
