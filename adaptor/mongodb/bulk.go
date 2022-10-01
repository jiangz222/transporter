package mongodb

import (
	"fmt"
	"sync"
	"time"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	maxBSONObjSize int = 16000000
)

var (
	_ client.Writer = &Bulk{}
)

// Bulk implements client.Writer for use with MongoDB and takes advantage of the Bulk API for
// performance improvements.
type Bulk struct {
	bulkMap map[string]*bulkOperation
	*sync.RWMutex
	confirmChan chan struct{}
	Set         bool
}

type bulkOperation struct {
	s          *mgo.Session
	bulk       *mgo.Bulk
	opCounter  int
	bsonOpSize int
}

func newBulker(done chan struct{}, wg *sync.WaitGroup, set bool) *Bulk {
	b := &Bulk{
		bulkMap: make(map[string]*bulkOperation),
		RWMutex: &sync.RWMutex{},
		Set:     set,
	}
	wg.Add(1)
	go b.run(done, wg)
	return b
}

func (b *Bulk) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		coll := msg.Namespace()
		b.Lock()
		b.confirmChan = msg.Confirms()
		bOp, ok := b.bulkMap[coll]
		if !ok {
			s := s.(*Session).mgoSession.Clone()
			bOp = &bulkOperation{
				s:    s,
				bulk: s.DB("").C(coll).Bulk(),
			}
			b.bulkMap[coll] = bOp
		}
		bs, err := bson.Marshal(msg.Data())
		if err != nil {
			log.Infof("unable to marshal doc to BSON, can't calculate size: %v", err)
		}
		// add the 4 bytes for the MsgHeader
		// https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#standard-message-header
		msgSize := len(bs) + 4

		// if the next op is going to put us over, flush and recreate bOp
		if bOp.opCounter >= s.(*Session).maxWriteBatchSize || bOp.bsonOpSize+msgSize >= maxBSONObjSize {
			err = b.flush(coll, bOp)
			if err != nil {
				log.With("collection", coll).Infof("error flushing collection that has reached its size capacity: %s\n", err.Error())
			}
			if err == nil && b.confirmChan != nil {
				b.confirmChan <- struct{}{}
			}
			s := s.(*Session).mgoSession.Clone()
			bOp = &bulkOperation{
				s:    s,
				bulk: s.DB("").C(coll).Bulk(),
			}
			b.bulkMap[coll] = bOp
		}
		if b.Set {
			bulkSet(msg, bOp)
		} else {
			fmt.Println("bulk write")
			bulkWrite(msg, bOp)
		}
		bOp.bsonOpSize += msgSize
		bOp.opCounter++
		b.Unlock()
		return msg, err
	}
}
func bulkWrite(msg message.Msg, bOp *bulkOperation) {
	switch msg.OP() {
	case ops.Delete:
		bOp.bulk.Remove(bson.M{"_id": msg.Data().Get("_id")})
	case ops.Insert:
		bOp.bulk.Insert(msg.Data())
	case ops.Update:
		bOp.bulk.Update(bson.M{"_id": msg.Data().Get("_id")}, msg.Data())

	}
}
func bulkSet(msg message.Msg, bOp *bulkOperation) {
	id, dataKey := getSetDataInfo(msg)
	fmt.Println("bulkset: ", id, dataKey)
	switch msg.OP() {
	case ops.Delete:
		bOp.bulk.Remove(bson.M{"_id": id})
	case ops.Insert:
		bOp.bulk.Upsert(bson.M{"_id": id}, bson.M{"$set": bson.M{dataKey: msg.Data()}})
	case ops.Update:
		bOp.bulk.Update(bson.M{"_id": id}, bson.M{"$set": bson.M{dataKey: msg.Data()}})
	}
}
func getSetDataInfo(msg message.Msg) (string, string) {
	dataKey := "nodeInfo"
	id := msg.Data().Get("nodeId")
	fmt.Println(id, dataKey)
	if id == nil {
		dataKey = "nodeStaticInfo"
		id = msg.Data().Get("_id")
	}
	fmt.Println(id, dataKey)
	return id.(string), dataKey
}

func (b *Bulk) run(done chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-time.After(2 * time.Second):
			if err := b.flushAll(); err != nil {
				log.Errorf("flush error, %s", err)
				return
			}
		case <-done:
			log.Infoln("received done channel")
			if err := b.flushAll(); err != nil {
				log.Errorf("flush error, %s", err)
			}
			return
		}
	}
}

func (b *Bulk) flushAll() error {
	b.Lock()
	for c, bOp := range b.bulkMap {
		if err := b.flush(c, bOp); err != nil {
			return err
		}
	}
	if b.confirmChan != nil {
		b.confirmChan <- struct{}{}
	}
	b.Unlock()
	return nil
}

// bOp.Bulk.actions:[{op:1 docs:[map[_id:002a3e275fb274a9795034643548d322,....]]}] 得到具体数据
func (b *Bulk) flush(c string, bOp *bulkOperation) error {
	log.Infof("****** %+v", c)
	log.With("collection", c).With("opCounter", bOp.opCounter).With("bsonOpSize", bOp.bsonOpSize).Infoln("flushing bulk messages")
	_, err := bOp.bulk.Run()
	if err != nil && !mgo.IsDup(err) {
		log.With("collection", c).Errorf("flush error, %s\n", err)
		return err
	} else if mgo.IsDup(err) {
		bOp.bulk.Unordered()
		if _, err := bOp.bulk.Run(); err != nil && !mgo.IsDup(err) {
			log.With("collection", c).Errorf("flush error with unordered, %s\n", err)
			return err
		}
	}
	bOp.s.Close()
	log.With("collection", c).Infoln("flush complete")
	delete(b.bulkMap, c)
	return nil
}
