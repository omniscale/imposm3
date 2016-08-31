package expire

import (
	"math"
	"sort"

	"github.com/omniscale/imposm3/geom/geojson"
)

// Calculate all tiles covered by the linear rings of the polygon
// and the tiles enclosed by it
func CoverPolygon(poly geojson.Polygon, zoom int) TileHash {
	if len(poly) == 0 {
		return TileHash{}
	}

	intersections := []TileFraction{}
	tiles := make(TileHash, 50)

	for _, linearRing := range poly {
		coveredTiles, ring := CoverLinestring(linearRing, zoom)
		tiles.MergeTiles(coveredTiles)

		j := 0
		k := len(ring) - 1
		for j < len(ring) {
			m := (j + 1) % len(ring)
			y := ring[j].Y

			localMinimum := y <= ring[k].Y && y <= ring[m].Y
			localMaximum := y >= ring[k].Y && y >= ring[m].Y
			isDuplicate := y == ring[m].Y
			if !localMinimum && !localMaximum && !isDuplicate {
				intersections = append(intersections, ring[j])
			}

			k = j
			j++
		}
	}

	sort.Sort(ByYX(intersections))

	for i := 0; i < len(intersections); i += 2 {
		// fill tiles between pairs of intersections
		y := intersections[i].Y
		for x := intersections[i].X + 1; x < intersections[i+1].X; x++ {
			tiles.AddTile(NewTile(x, y, zoom))
		}
	}
	return tiles
}

// Calculate all tiles covered by linestring
func CoverLinestring(points geojson.LineString, zoom int) (TileHash, []TileFraction) {
	tiles := make(TileHash)
	ring := []TileFraction{}
	prev := TileFraction{}

	var x, y float64
	for i := 0; i < len(points)-1; i++ {
		start := ToTileFraction(points[i], zoom)
		stop := ToTileFraction(points[i+1], zoom)

		//Calculate distance between points
		d := TileFraction{stop.X - start.X, stop.Y - start.Y}

		//Skip if start and stop are the same
		if d.Y == 0 && d.X == 0 {
			continue
		}

		x = math.Floor(start.X)
		y = math.Floor(start.Y)

		//Check if we already found the tile for this way
		sameAsPrevious := x == prev.X && y == prev.Y
		if !sameAsPrevious {
			tiles.AddTile(NewTile(x, y, zoom))
			ring = append(ring, TileFraction{x, y})
			prev = TileFraction{x, y}
		}

		//TODO: What is sx?
		sx := -1.0
		if d.X > 0 {
			sx = 1.0
		}
		sy := -1.0
		if d.Y > 0 {
			sy = 1.0
		}

		tMaxX := math.Abs((x - start.X) / d.X)
		if d.X > 0 {
			tMaxX = math.Abs((1 + x - start.X) / d.X)
		}

		tMaxY := math.Abs((y - start.Y) / d.Y)
		if d.Y > 0 {
			tMaxY = math.Abs((1 + y - start.Y) / d.Y)
		}

		td := TileFraction{math.Abs(sx / d.X), math.Abs(sy / d.Y)}
		for tMaxX < 1 || tMaxY < 1 {
			if tMaxX < tMaxY {
				tMaxX += td.X
				x += sx
			} else {
				tMaxY += td.Y
				y += sy
			}

			tiles.AddTile(NewTile(x, y, zoom))
			if y != prev.Y {
				ring = append(ring, TileFraction{x, y})
			}

			prev = TileFraction{x, y}
		}
	}

	if y == ring[0].Y {
		ring = ring[:len(ring)-1]
	}

	return tiles, ring
}

// Calculate all tiles covered by the point
func CoverPoint(p geojson.Point, zoom int) Tile {
	tf := ToTileFraction(p, zoom)
	return NewTile(tf.X, tf.Y, zoom)
}

func ToTileFraction(p geojson.Point, zoomLevel int) TileFraction {
	d2r := math.Pi / 180
	z2 := math.Pow(2, float64(zoomLevel))
	sin := math.Sin(p.Lat * d2r)

	return TileFraction{
		X: z2 * (p.Long/360 + 0.5),
		Y: z2 * (0.5 - 0.25*math.Log((1+sin)/(1-sin))/math.Pi),
	}
}
