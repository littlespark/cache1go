package nutsdb

import (
	"github.com/xujiajun/nutsdb"
	"log"
)

type Nuts struct {}
var (
	nutsDB *nutsdb.DB
	_bucket = "bucket1"
	_dir = "/tmp/nutsdb0"
)

func init() {
	opt := nutsdb.DefaultOptions
	opt.Dir = _dir
	db, err := nutsdb.Open(opt)
	if err != nil {
		log.Fatal(err)
	}
	nutsDB = db

}

//set key value ttl
func (n *Nuts) Write(key,value []byte, ttl uint32 ) {
	if err := nutsDB.Update(
		func(tx *nutsdb.Tx) error {
			//key := []byte("k1")
			//val := []byte("v1")
			bucket := _bucket
			// 如果设置 ttl = 0 or Persistent, 这个key就会永久不删除
			// 这边 ttl = 60 , 60s之后就会过期。
			if err := tx.Put(bucket, key, value, ttl); err != nil {
				return err
			}

			log.Printf("------  nutsdb SET %s %s", key, value)
			return nil
		}); err != nil {
			log.Fatal(err)
	}
}


func (n *Nuts) Read(key []byte) string {
	value:=""
	if err := nutsDB.View(
		func(tx *nutsdb.Tx) error {
			if e, err := tx.Get(_bucket, key); err != nil {
				value = "(nil)" //err.Error()
			} else {
				value = string(e.Value)
				//fmt.Println(string(e.Value), e.Meta.TTL) // "val1-modify"
			}

			log.Printf("------ nutsdb GET %s%s", key, value)

			return nil
		}); err != nil {
		log.Println(err)
		}
		return value
}

func (n *Nuts) Close() {
	nutsDB.Close()
}