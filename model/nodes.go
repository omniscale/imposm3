package model

import "code.google.com/p/goprotobuf/proto"
import "log"

// type Node struct {
// 	Id   int64
// 	Tags map[string]string
// 	Lon  uint32
// 	Lat  uint32
// }

const COORD_FACTOR float64 = 11930464.7083 // ((2<<31)-1)/360.0

func coordToInt(coord float64) uint32 {
	return uint32((coord + 180.0) * COORD_FACTOR)
}

func intToCoord(coord uint32) float64 {
	return float64((float64(coord) / COORD_FACTOR) - 180.0)
}

func (this *Node) WgsCoord() (lon float64, lat float64) {
	lon = intToCoord(this.GetLong())
	lat = intToCoord(this.GetLat())
	return
}

func (this *Node) FromWgsCoord(lon float64, lat float64) {
	longInt := coordToInt(lon)
	latInt := coordToInt(lat)
	this.Long = &longInt
	this.Lat = &latInt
}

func (this *Way) Marshal() []byte {
	data, err := proto.Marshal(this)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	return data
}
