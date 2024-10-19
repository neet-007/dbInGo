package bplustree

import (
	"bytes"
	"encoding/binary"
)

/*
FORMAT:

| type | nkeys |  pointers  |   offsets  | key-values | unused |
|  2B  |   2B  | nkeys * 8B | nkeys * 2B |     ...    |        |

This is the format of each KV pair. Lengths followed by data.


| klen | vlen | key | val |
|  2B  |  2B  | ... | ... |
*/

const (
	BNODE_NODE = 1
	BNODE_LEAF = 2
)

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

func (node BNode) getPtr(idx uint16) uint64 {
	if idx >= node.nkeys() {
		panic("the index is bigger than the num of keys")
	}

	return binary.LittleEndian.Uint64(node[idx:])
}

func (node BNode) setPtr(idx uint16, ptr uint64) {
	if idx >= node.nkeys() {
		panic("the index is bigger than the num of keys")
	}

	start := idx * 8

	binary.LittleEndian.PutUint64(node[start:], ptr)
}

func offsetPos(node BNode, idx uint16) uint16 {
	if 1 > idx || idx > node.nkeys() {
		panic("the idx cannot be less then 1 or bigger then node num of keys")
	}

	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}

	return binary.LittleEndian.Uint16(node[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node[offsetPos(node, idx):], offset)
}

func (node BNode) kvPos(idx uint16) uint16 {
	if idx > node.nkeys() {
		panic("idx cannot be bigger than node num of keys")
	}

	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	if idx >= node.nkeys() {
		panic("idx cannot be bigger than or equal to node num of keys")
	}

	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])

	return node[pos+4:][:klen]

}

func (node BNode) getVal(idx uint16) []byte {
	if idx >= node.nkeys() {
		panic("idx cannot be bigger than or equal to node num of keys")
	}

	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])

	return node[pos+4+klen:][:vlen]
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// returns the first kid node whose range intersects the key. (kid[i] <= key)
// TODO: binary search

func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)

	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)

		if cmp <= 0 {
			found = i
		}

		if cmp >= 0 {
			break
		}
	}

	return found
}

func leafInsert(
	new BNode, old BNode, idx uint16,
	key []byte, val []byte,
) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppedKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-1)
}

func leafUpdate(
	new BNode, old BNode, idx uint16,
	key []byte, val []byte,
) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppedKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-idx-1)
}

func nodeAppedKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	new.setPtr(idx, ptr)

	pos := new.kvPos(idx)

	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))

	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)

	new.setOffset(idx+1, new.getOffset(idx)+4+uint16(len(key)+len(val)))
}

// copy multiple KVs into the position from the old node
func nodeAppendRange(
	new BNode, old BNode,
	dstNew uint16, srcOld uint16, n uint16,
) {
	for i := uint16(0); i < n; i++ {
		nodeAppedKV(new, dstNew+i, old.getPtr(srcOld+i), old.getKey(srcOld+i), old.getVal(srcOld+i))
	}
}

func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, idx uint16,
	kids ...BNode,
) {
	inc := uint16(len(kids))

	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)

	for i, node := range kids {
		nodeAppedKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}

	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))

}

func findSplitIndex(old BNode) uint16 {
	var nbytes uint16 = 0

	for i := uint16(0); i < old.nkeys(); i++ {
		pos := old.kvPos(i)

		keyLen := binary.LittleEndian.Uint16(old[pos : pos+2])
		valLen := binary.LittleEndian.Uint16(old[pos+2 : pos+4])

		nbytes += 4 + keyLen + valLen
		if nbytes > BTREE_PAGE_SIZE {
			return i
		}
	}

	return old.nkeys() / 2
}

// split a oversized node into 2 so that the 2nd node always fits on a page
func nodeSplit2(left BNode, right BNode, old BNode) {
	splitIdx := findSplitIndex(old)

	nodeAppendRange(right, old, 0, 0, splitIdx)

	nodeAppendRange(left, old, 0, splitIdx, old.nkeys()-splitIdx)

	right.setHeader(BNODE_NODE, splitIdx)
	left.setHeader(BNODE_NODE, old.nkeys()-splitIdx)
}

// split a node if it's too big. the results are 1~3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}

	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	right := BNode(make([]byte, BTREE_PAGE_SIZE))

	nodeSplit2(left, right, old)

	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	leftleft := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))

	nodeSplit2(leftleft, middle, left)
	if leftleft.nbytes() > BTREE_PAGE_SIZE {
		panic("spillting the node failed the num of bytes in leftleft should nevert be more than BTREE_PAGE_SIZE")
	}

	leftleft = leftleft[:BTREE_PAGE_SIZE]
	return 3, [3]BNode{leftleft, middle, right}
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))

	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(new, node, idx, key, val)
		} else {
			leafInsert(new, node, idx, key, val)
		}
	case BNODE_NODE:
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("bad node")
	}

	return new
}

func nodeInsert(tree *BTree, new BNode, node BNode, idx uint16, key []byte, val []byte) {
	kptr := node.getPtr(idx)

	knode := treeInsert(tree, tree.get(kptr), key, val)

	nsplit, split := nodeSplit3(knode)
	tree.del(kptr)

	nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
}
