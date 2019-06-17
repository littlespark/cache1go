
package cache

import (
	"sort"
	"strings"
	"sync"
	"time"
)


func New() *cache{
	return &cache{}
}

type cache struct {
	container sync.Map
	items items
}

type items struct {
	value    interface{}
	expireAt int64
}

func (c *cache) Set(key string, value interface{}, sec int64) {
	expireAt := int64(9223372036854775807)
	if sec != -1 {
		expireAt = time.Now().Unix() + sec
	}

	c.container.Store(key, items{
		value: value,
		expireAt:expireAt,
	})
}

func (c *cache) Get(key string) (value interface{}){

	v, _ := c.container.Load(key)

	if v == nil {
		return nil
	}

	if int64(v.(items).expireAt) <= time.Now().Unix() {
		return nil
	}

	return v.(items).value
}

func (c *cache) Exists(key string) (isFound bool){
	v,_ := c.container.Load(key)

	if v == nil {
		return false
	}

	if int64(v.(items).expireAt) <= time.Now().Unix() {
		return false
	}
	if _,f := c.container.Load(key);f {
		return true
	}
	return false
}

func (c *cache) Del(key string) bool{
	c.container.Delete(key)
	if c.Exists(key) {
		return false
	}
	return true
}

func (c *cache) ExpireAt(key string) int64{
	v,_ := c.container.Load(key)
	return int64(v.(items).expireAt)
}


type retStruct struct {
	key string
	value interface{}
	expireAt int64
}
func (c *cache) GetAll()  []retStruct{
	ret := []retStruct{}
	c.container.Range(func(k, v interface{}) bool {
		ret = append(ret,retStruct{
			key:      k.(string),
			value:    v.(items).value,
			expireAt: v.(items).expireAt,
		})
		return true
	})
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].key < ret[j].key
	})
	return ret
}


func (c *cache) Keys(pattern string) []string {
	keys := []string{}
	c.container.Range(func(k, v interface{}) bool {
		if pattern == "*" {
			keys = append(keys, k.(string))
		}else {
			if strings.Contains(k.(string), pattern) == true {
				keys = append(keys, k.(string))
			}
		}
		return true
	})
	sort.Strings(keys)
	return keys
}


//返回当前数据库的 key 的数量
func (c *cache) DBSize() int {
	len := 0
	c.container.Range(func(k, v interface{}) bool {
		len++
		return true
	})
	return len
}

func (c *cache) FlushAll() {
	c.container.Range(func(k, v interface{}) bool {
		c.container.Delete(k)
		return true
	})
}