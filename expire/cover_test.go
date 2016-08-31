package expire

import (
	"reflect"
	"testing"
)

// https://github.com/mapbox/tile-cover/blob/master/test/fixtures/building.geojson
var Building = []Point{
	Point{-77.15269088745116, 38.87153962460514},
	Point{-77.1521383523941, 38.871322446566325},
	Point{-77.15196132659912, 38.87159391901113},
	Point{-77.15202569961546, 38.87162315444336},
	Point{-77.1519023180008, 38.87179021382536},
	Point{-77.15266406536102, 38.8727758561868},
	Point{-77.1527713537216, 38.87274662122871},
	Point{-77.15282499790192, 38.87282179681094},
	Point{-77.15323269367218, 38.87267562199469},
	Point{-77.15313613414764, 38.87254197618533},
	Point{-77.15270698070526, 38.87236656567917},
	Point{-77.1523904800415, 38.87198233162923},
	Point{-77.15269088745116, 38.87153962460514},
}

func comparePointTile(t *testing.T, lon, lat float64, x, y, z int) {
	expected := Tile{x, y, z}
	actual := CoverPoint(lon, lat, z)
	if actual != expected {
		t.Error("Expected ", expected, ", got ", actual)
	}
}

func TestPointToTile(t *testing.T) {
	// Knie's Kinderzoo in Rapperswil, Switzerland
	comparePointTile(t, 8.8223, 47.2233, 8593, 5747, 14)
}

func TestLongCoverLinestring(t *testing.T) {
	points := []Point{
		Point{-106.21719360351562, 28.592359801121567},
		Point{-106.1004638671875, 28.791130513231813},
		Point{-105.87661743164062, 28.864519767126602},
		Point{-105.82374572753905, 28.60743139267596},
	}
	tiles, _ := CoverLinestring(points, 10)

	expectedTiles := FromTiles([]Tile{
		Tile{209, 427, 10},
		Tile{209, 426, 10},
		Tile{210, 426, 10},
		Tile{210, 427, 10},
	})

	if !reflect.DeepEqual(tiles, expectedTiles) {
		t.Error("Unexpected tiles", tiles.ToTiles())
	}
}

// Test a spiked Polygon with many intersections
// https://github.com/mapbox/tile-cover/blob/master/test/fixtures/spiked.geojson
func TestCoverSpikePolygon(t *testing.T) {
	points := []Point{
		Point{16.611328125, 8.667918002363134},
		Point{13.447265624999998, 3.381823735328289},
		Point{15.3369140625, -6.0968598188879355},
		Point{16.7431640625, 1.0546279422758869},
		Point{18.193359375, -10.314919285813147},
		Point{19.248046875, -1.4061088354351468},
		Point{20.698242187499996, -4.565473550710278},
		Point{22.587890625, 0.3515602939922709},
		Point{24.2138671875, -11.73830237143684},
		Point{29.091796875, 5.003394345022162},
		Point{26.4990234375, 9.752370139173285},
		Point{26.0595703125, 7.623886853120036},
		Point{24.9169921875, 9.44906182688142},
		Point{22.587890625, 6.751896464843375},
		Point{21.665039062499996, 12.597454504832017},
		Point{20.9619140625, 8.189742344383703},
		Point{18.193359375, 14.3069694978258},
		Point{16.611328125, 8.667918002363134},
	}
	tiles := CoverPolygon(points, 6)
	expectedTiles := FromTiles([]Tile{
		Tile{35, 29, 6},
		Tile{34, 30, 6},
		Tile{35, 30, 6},
		Tile{36, 30, 6},
		Tile{37, 30, 6},
		Tile{34, 31, 6},
		Tile{35, 31, 6},
		Tile{36, 31, 6},
		Tile{37, 31, 6},
		Tile{34, 32, 6},
		Tile{35, 32, 6},
		Tile{36, 32, 6},
		Tile{34, 33, 6},
		Tile{35, 33, 6},
		Tile{36, 33, 6},
		Tile{36, 34, 6},
	})
	if !reflect.DeepEqual(tiles, expectedTiles) {
		t.Error("Unexpected tiles", tiles.ToTiles())
	}
}

func TestCoverLinestringLinearRing(t *testing.T) {
	tiles, ring := CoverLinestring(Building, 20)
	expectedTiles := FromTiles([]Tile{
		Tile{299564, 401224, 20},
		Tile{299564, 401225, 20},
		Tile{299565, 401225, 20},
		Tile{299566, 401225, 20},
		Tile{299566, 401224, 20},
		Tile{299566, 401223, 20},
		Tile{299566, 401222, 20},
		Tile{299565, 401222, 20},
		Tile{299565, 401221, 20},
		Tile{299564, 401221, 20},
		Tile{299564, 401220, 20},
		Tile{299563, 401220, 20},
		Tile{299562, 401220, 20},
		Tile{299563, 401221, 20},
		Tile{299564, 401222, 20},
		Tile{299565, 401223, 20},
		Tile{299564, 401223, 20},
	})

	if !reflect.DeepEqual(tiles, expectedTiles) {
		t.Error("Unexpected tiles", tiles.ToTiles())
	}
	expectedRing := []TileFraction{
		TileFraction{299564, 401224},
		TileFraction{299564, 401225},
		TileFraction{299566, 401224},
		TileFraction{299566, 401223},
		TileFraction{299566, 401222},
		TileFraction{299565, 401221},
		TileFraction{299564, 401220},
		TileFraction{299563, 401221},
		TileFraction{299564, 401222},
		TileFraction{299565, 401223},
	}
	if !reflect.DeepEqual(ring, expectedRing) {
		t.Error("Unexpected ring", tiles.ToTiles())
	}
}

// Test a building polygon
func TestCoverPolygonBuilding(t *testing.T) {
	tiles := CoverPolygon(Building, 20)
	expectedTiles := FromTiles([]Tile{
		Tile{299565, 401224, 20},
		Tile{299564, 401224, 20},
		Tile{299564, 401225, 20},
		Tile{299565, 401225, 20},
		Tile{299566, 401225, 20},
		Tile{299566, 401224, 20},
		Tile{299566, 401223, 20},
		Tile{299566, 401222, 20},
		Tile{299565, 401222, 20},
		Tile{299565, 401221, 20},
		Tile{299564, 401221, 20},
		Tile{299564, 401220, 20},
		Tile{299563, 401220, 20},
		Tile{299562, 401220, 20},
		Tile{299563, 401221, 20},
		Tile{299564, 401222, 20},
		Tile{299565, 401223, 20},
		Tile{299564, 401223, 20},
	})
	if !reflect.DeepEqual(tiles, expectedTiles) {
		t.Error("Unexpected tiles for building polygon", tiles.ToTiles())
	}
}

func TestCoverPolygon(t *testing.T) {
	ring := []Point{
		Point{-79.9365234375, 32.77212032198862},
		Point{-79.9306869506836, 32.77212032198862},
		Point{-79.9306869506836, 32.776811185047144},
		Point{-79.9365234375, 32.776811185047144},
		Point{-79.9365234375, 32.77212032198862},
	}
	tiles := CoverPolygon(ring, 16)

	expectedTiles := FromTiles([]Tile{
		Tile{18216, 26447, 16},
		Tile{18217, 26447, 16},
		Tile{18217, 26446, 16},
		Tile{18216, 26446, 16},
	})
	if !reflect.DeepEqual(tiles, expectedTiles) {
		t.Error("Unexpected tiles", tiles.ToTiles())
	}
}

func TestCoverLinestring(t *testing.T) {
	points := []Point{
		Point{8.826313018798828, 47.22796198584928},
		Point{8.82596969604492, 47.20755789924751},
		Point{8.826141357421875, 47.194845099780174},
	}

	tiles, _ := CoverLinestring(points, 14)
	expectedTiles := FromTiles([]Tile{
		Tile{8593, 5747, 14},
		Tile{8593, 5748, 14},
		Tile{8593, 5749, 14},
	})

	if !reflect.DeepEqual(tiles, expectedTiles) {
		t.Error("Unexpected tiles", tiles.ToTiles())
	}
}
