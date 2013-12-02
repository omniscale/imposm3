package proj

import (
	"math"
	"testing"
)

func TestWgsToMerc(t *testing.T) {
	x, y := WgsToMerc(0, 0)
	if x != 0 || y != 0 {
		t.Fatalf("%v %v", x, y)
	}

	x, y = WgsToMerc(8, 53)
	if math.Abs(x-890555.9263461898) > 1e-6 || math.Abs(y-6982997.920389788) > 1e-6 {
		t.Fatalf("%v %v", x, y)
	}
}

func TestMercToWgs(t *testing.T) {
	long, lat := MercToWgs(0, 0)
	if long != 0 || lat != 0 {
		t.Fatalf("%v %v", long, lat)
	}
	long, lat = MercToWgs(890555.9263461898, 6982997.920389788)
	if math.Abs(long-8) > 1e-6 || math.Abs(lat-53) > 1e-6 {
		t.Fatalf("%v %v", long, lat)
	}
}
