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

func (t Tile) Key() string {
	return strings.Join([]string{strconv.Itoa(t.X), strconv.Itoa(t.Y), strconv.Itoa(t.Z)}, "/")
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

func PointToTile(lon, lat float64, zoomLevel int) Tile {
	tf := PointToTileFraction(lon, lat, zoomLevel)
	return Tile{
		X: int(tf.X),
		Y: int(tf.Y),
		Z: int(tf.Z),
	}
}
