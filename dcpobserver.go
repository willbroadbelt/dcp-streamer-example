package main

import (
	"fmt"
	"github.com/couchbase/gocbcore/v9"
	"sync"
)

type Mutation struct {
	Expiry       uint32
	Locktime     uint32
	Cas          uint64
	Value        []byte
	CollectionID uint32
	StreamID     uint16
}

type Deletion struct {
	IsExpiration bool
	DeleteTime   uint32
	Cas          uint64
	CollectionID uint32
	StreamID     uint16
}

type SnapshotMarker struct {
	lastSnapStart uint64
	lastSnapEnd   uint64
}

type SeqnoMarker struct {
	startSeqNo uint64
	endSeqNo   uint64
}

type DCPStreamObserver struct {
	lock           sync.Mutex
	mutations      map[string]Mutation //TODO: Could use struct with {counter, Lock} for each thing so we don't lock everything
	deletions      map[string]Deletion
	expirations    map[string]Deletion
	collCreations  uint32
	scopeCreations uint32
	collDels       uint32
	scopeDels      uint32
	dataRange      map[uint16]SeqnoMarker
	lastSeqno      map[uint16]uint64
	snapshots      map[uint16]SnapshotMarker
	endWg          sync.WaitGroup
}

func (so *DCPStreamObserver) SnapshotMarker(startSeqNo, endSeqNo uint64, vbId uint16, streamId uint16,
	snapshotType gocbcore.SnapshotState) {
	//println("SNAPSHOT MARKER")
	so.lock.Lock()
	so.snapshots[vbId] = SnapshotMarker{startSeqNo, endSeqNo}
	if so.lastSeqno[vbId] < startSeqNo || so.lastSeqno[vbId] > endSeqNo {
		so.lastSeqno[vbId] = startSeqNo
	}
	so.lock.Unlock()
}

func (so *DCPStreamObserver) Mutation(seqNo, revNo uint64, flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbId uint16,
	collectionId uint32, streamId uint16, key, value []byte) {
	mutation := Mutation{
		Expiry:       expiry,
		Locktime:     lockTime,
		Cas:          cas,
		Value:        value,
		CollectionID: collectionId,
		StreamID:     streamId,
	}
	so.lock.Lock()
	so.mutations[string(key)] = mutation
	fmt.Printf("Mutations Total: %d", len(so.mutations))
	so.lock.Unlock()
}

func (so *DCPStreamObserver) Deletion(seqNo, revNo uint64, deleteTime uint32, cas uint64, datatype uint8, vbId uint16, collectionId uint32, streamId uint16,
	key, value []byte) {
	deletion := Deletion{
		IsExpiration: false,
		DeleteTime:   deleteTime,
		Cas:          cas,
		CollectionID: collectionId,
		StreamID:     streamId,
	}
	println("DELETION")

	so.lock.Lock()
	so.deletions[string(key)] = deletion
	so.lock.Unlock()
}

func (so *DCPStreamObserver) Expiration(seqNo, revNo uint64, deleteTime uint32, cas uint64, vbId uint16, collectionId uint32, streamId uint16, key []byte) {
	expiration := Deletion{
		IsExpiration: true,
		DeleteTime:   deleteTime,
		Cas:          cas,
		CollectionID: collectionId,
		StreamID:     streamId,
	}

	so.lock.Lock()
	so.deletions[string(key)] = expiration
	so.lock.Unlock()
}

func (so *DCPStreamObserver) End(vbId uint16, streamId uint16, err error) {
	//fmt.Println("END")
	so.endWg.Done()
}

func (so *DCPStreamObserver) CreateCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32,
	collectionId uint32, ttl uint32, streamId uint16, key []byte) {
	//fmt.Printf("Collection Created: %d\n", collectionId)
	so.lock.Lock()
	so.collCreations++
	fmt.Printf("Collections Total: %d\n", so.collCreations)
	so.lock.Unlock()
}

func (so *DCPStreamObserver) DeleteCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32,
	collectionId uint32, streamId uint16) {
	fmt.Printf("Collection Deleted: %d\n", collectionId)
}

func (so *DCPStreamObserver) FlushCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64,
	collectionId uint32) {
	fmt.Printf("Collection Flushed: %d\n", collectionId)
}

func (so *DCPStreamObserver) CreateScope(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32,
	streamId uint16, key []byte) {
	//fmt.Printf("Scope Created: %d\n", scopeId)
	so.lock.Lock()
	so.scopeCreations++
	fmt.Printf("Total scopes: %d\n", so.scopeCreations)
	so.lock.Unlock()
}

func (so *DCPStreamObserver) DeleteScope(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32,
	streamId uint16) {
	fmt.Printf("Scope Deleted: %d\n", scopeId)
}

func (so *DCPStreamObserver) ModifyCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64,
	collectionId uint32, ttl uint32, streamId uint16) {
	fmt.Printf("Modified Collection: %d\n", collectionId)
}

func (so *DCPStreamObserver) OSOSnapshot(vbId uint16, snapshotType uint32, streamID uint16) {
	println("OSO_SS")
}

func (so *DCPStreamObserver) SeqNoAdvanced(vbId uint16, bySeqNo uint64, streamID uint16) {
	println("SEQNO ADVANCED")
	so.lastSeqno[vbId] = bySeqNo
}
