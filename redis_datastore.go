package godsredis

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/go-redis/redis"
	datastore "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
)

//Datastore is a in memory datastore based on redis
type Datastore struct {
	mu     sync.Mutex
	client *redis.Client
}

//NewDatastore returns  datastore based on redis
func NewDatastore(client *redis.Client) (*Datastore, error) {
	return &Datastore{
		client: client,
	}, nil
}

//Put method adds the given key,value to redis SET
func (ds *Datastore) Put(key datastore.Key, value []byte) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	_, err := ds.client.SAdd(key.String(), value).Result()
	if err != nil {
		log.Println(err)
		return err
	}
	log.Printf("%s is added", key.String())
	return nil
}

//Sync is TODO
func (ds *Datastore) Sync(prefix datastore.Key) error {
	return nil
}

//Get method returns values associated with key as byte array
func (ds *Datastore) Get(key datastore.Key) (value []byte, err error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	result, err := ds.client.SMembers(key.String()).Result()
	if err != nil {
		log.Println(err)
	}
	return json.Marshal(result)
}

//GetSize returns the  no.of elements mapped to this key
func (ds *Datastore) GetSize(key datastore.Key) (size int, err error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	res, err := ds.client.SCard(key.String()).Result()
	return int(res), err
}

//Has method checks wheather the given key is available or not using SCard method of redis set
func (ds *Datastore) Has(key datastore.Key) (exists bool, err error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	res, err := ds.client.SCard(key.String()).Result()
	if err != nil {
		log.Println(err)
		return false, err
	}
	if res == 0 {
		return false, nil
	}

	return true, nil
}

//Delete removes the key and associated values in redis set
func (ds *Datastore) Delete(key datastore.Key) (err error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	log.Println(key.String())
	return ds.client.Del(key.String()).Err()
}

//Query not implemented
func (ds *Datastore) Query(q query.Query) (query.Results, error) {

	keys, err := ds.client.Keys("*").Result()
	if err != nil {
		return nil, err
	}
	re := make([]query.Entry, 0, len(keys))
	for _, k := range keys {
		v, errs := json.Marshal(ds.client.SMembers(k))
		if errs != nil {
			return nil, errs
		}
		e := query.Entry{Key: k, Size: len(k)}
		if !q.KeysOnly {
			e.Value = v
		}
		re = append(re, e)
	}

	r := query.ResultsWithEntries(q, re)
	r = query.NaiveQueryApply(q, r)
	return r, nil

	// return nil, errors.New("TODO implement query for redis datastore?")
}

//Batch is not supported
func (ds *Datastore) Batch() (datastore.Batch, error) {
	return datastore.NewBasicBatch(ds), nil
	//return nil, datastore.ErrBatchUnsupported
}

//Close method close the redis client
func (ds *Datastore) Close() error {
	return ds.client.Close()
}
