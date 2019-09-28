package util

func Uint16ToBigEndBytes(u uint16) (r []byte) {
	r = append(r, byte(u>>8), byte(u&0xff))
	return
}

func BigEndBytesToUint16(b []byte) uint16 {
	Assert.Equal(len(b), 2)
	return uint16((b[0] << 8) | b[1])
}

func Uint32ToBigEndBytes(u uint32) (r []byte) {
	return append(r, byte(u>>24), byte(u>>16&0xff), byte(u>>8&0xff), byte(u&0xff))
}

func BigEndBytesToUint32(b []byte) uint32 {
	Assert.Equal(len(b), 4)
	return uint32(b[0]<<24 | b[1]<<16 | b[2]<<8 | b[3])
}
