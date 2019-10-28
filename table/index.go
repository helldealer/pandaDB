package table

import (
	"pandadb/util"
)

type FileIndex struct {
	begin  string
	end    string
	kvMap  map[string]*ValueInfo
	filter *BloomFiler
}

func NewFileIndex(begin, end string, kvMap map[string]*ValueInfo) *FileIndex {
	fi := &FileIndex{
		begin: begin,
		end:   end,
		kvMap: make(map[string]*ValueInfo),
	}
	fi.kvMap = kvMap
	return fi
}

func NewFileIndexFromFile(index []byte) *FileIndex {
	fi := &FileIndex{
		kvMap: make(map[string]*ValueInfo),
	}
	if !(index[0] == 'l' && index[1] == 'h' && index[2] == 'r') {
		panic("magic num is incorrect")
	}
	//todo: 所有的硬编码换成参数
	first := true
	in := index[2:]
	for {
		l := util.BigEndBytesToUint16(in)
		v := &ValueInfo{
			util.BigEndBytesToUint32(in[2+l:]),
			util.BigEndBytesToUint16(in[2+l+4:]),
		}
		k := string(in[2 : l+2])
		fi.kvMap[k] = v
		if first {
			fi.begin = k
			first = false
		}
		in = in[2+l+4+2:]
		if len(in) == 0 {
			fi.end = k
			break
		}
	}
	return fi
}

type ValueInfo struct {
	pos uint32
	len uint16
}

func NewValueInfo(pos uint32, len uint16) *ValueInfo{
	return &ValueInfo{pos, len}
}

type BloomFiler struct {
}

type ValueInfoWithFile struct {
	info *ValueInfo
	index int
}
