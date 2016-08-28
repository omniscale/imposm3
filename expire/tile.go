package expire

import (
	"math"
	"strconv"
	"strings"
)

type Tile struct {
	X int
	Y int
	Z int
}

func (t Tile) toID() int {
	dim := 2 * (1 << uint(t.Z))
	return ((dim*t.Y + t.X) * 32) + t.Z
}

func (t Tile) Key() string {
	return strings.Join([]string{strconv.Itoa(t.X), strconv.Itoa(t.Y), strconv.Itoa(t.Z)}, "/")
}

func fromID(id int) Tile {
	z := id % 32
	dim := 2 * (1 << uint(z))
	xy := ((id - z) / 32)
	x := xy % dim
	y := ((xy - x) / dim) % dim
	return Tile{x, y, z}
}

type TileFraction struct {
	X float64
	Y float64
	Z int
}

func PointToTileFraction(lon, lat float64, zoomLevel int) TileFraction {
	d2r := math.Pi / 180
	z2 := math.Pow(2, float64(zoomLevel))
	sin := math.Sin(lat * d2r)

	return TileFraction{
		X: z2 * (lon/360 + 0.5),
		Y: z2 * (0.5 - 0.25*math.Log((1+sin)/(1-sin))/math.Pi),
		Z: zoomLevel,
	}
}

type Point struct {
	lon float64
	lat float64
}

func LinestringToTile(points []Point, maxZoom int) []Tile {
	tiles := []Tile{}
	var prevX float64
	var prevY float64

	for i := 0; i < len(points)-1; i++ {
		start := PointToTileFraction(points[i].lon, points[i].lat, maxZoom)
		stop := PointToTileFraction(points[i+1].lon, points[i+1].lat, maxZoom)

		x0 := start.X
		y0 := start.Y
		x1 := stop.X
		y1 := stop.Y
		dx := x1 - x0
		dy := y1 - y0

		if dy == 0 && dx == 0 {
			continue
		}

		sx := -1.0
		if dx > 0 {
			sx = 1.0
		}
		sy := -1.0
		if dy > 0 {
			sy = 1.0
		}

		x := math.Floor(x0)
		y := math.Floor(y0)

		tMaxX := math.Abs((x - x0) / dx)
		if dx > 0 {
			tMaxX = math.Abs((1 + x - x0) / dx)
		}

		tMaxY := math.Abs((y - y0) / dy)
		if dy > 0 {
			tMaxY = math.Abs((1 + y - y0) / dy)
		}

		tdx := math.Abs(sx / dx)
		tdy := math.Abs(sy / dy)

		if x != prevX || y != prevY {
			tiles = append(tiles, Tile{int(x), int(y), maxZoom})
			prevX = x
			prevY = y
		}

		for tMaxX < 1 || tMaxY < 1 {
			if tMaxX < tMaxY {
				tMaxX += tdx
				x += sx
			} else {
				tMaxY += tdy
				y += sy
			}

			tiles = append(tiles, Tile{int(x), int(y), maxZoom})
			prevX = x
			prevY = y
		}
	}
	return tiles
}

func PointToTile(lon, lat float64, zoomLevel int) Tile {
	tf := PointToTileFraction(lon, lat, zoomLevel)
	return Tile{
		X: int(tf.X),
		Y: int(tf.Y),
		Z: int(tf.Z),
	}
}
