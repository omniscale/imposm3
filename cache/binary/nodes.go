package binary

func (nd *Node) wgsCoord() (lon float64, lat float64) {
	lon = IntToCoord(nd.GetLong())
	lat = IntToCoord(nd.GetLat())
	return
}

func (nd *Node) fromWgsCoord(lon float64, lat float64) {
	nd.Long = CoordToInt(lon)
	nd.Lat = CoordToInt(lat)
}
