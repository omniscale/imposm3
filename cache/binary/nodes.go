package binary

func (this *Node) wgsCoord() (lon float64, lat float64) {
	lon = IntToCoord(this.GetLong())
	lat = IntToCoord(this.GetLat())
	return
}

func (this *Node) fromWgsCoord(lon float64, lat float64) {
	longInt := CoordToInt(lon)
	latInt := CoordToInt(lat)
	this.Long = &longInt
	this.Lat = &latInt
}
