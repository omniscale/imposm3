package binary

import "code.google.com/p/goprotobuf/proto"
import "log"

func (this *Node) WgsCoord() (lon float64, lat float64) {
	lon = IntToCoord(this.GetLong())
	lat = IntToCoord(this.GetLat())
	return
}

func (this *Node) FromWgsCoord(lon float64, lat float64) {
	longInt := CoordToInt(lon)
	latInt := CoordToInt(lat)
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
