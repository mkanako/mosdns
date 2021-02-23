package lazy_cache

import (
	"context"
	"time"

	"github.com/IrineSistiana/mosdns/dispatcher/pkg/pool"
	"github.com/go-redis/redis/v8"
	"github.com/miekg/dns"
	"github.com/mitchellh/mapstructure"
)

type redisCache struct {
	client *redis.Client
}

func newRedisCache(url string) (*redisCache, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	c := redis.NewClient(opt)
	return &redisCache{client: c}, nil
}

func (r *redisCache) get(ctx context.Context, key string) (v *dns.Msg, ttl uint32, expire int64, err error) {
	b, err := r.client.HGetAll(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, 0, 0, nil
		}
		return nil, 0, 0, err
	}
	if len(b) == 0 {
		return nil, 0, 0, nil
	}
	data := new(CacheData)
	mapstructure.WeakDecode(b, data)

	v = new(dns.Msg)
	if err := v.Unpack(data.Msg); err != nil {
		return nil, 0, 0, err
	}
	return v, data.Ttl, data.Expire, nil
}

func (r *redisCache) store(ctx context.Context, key string, v *dns.Msg, ttl uint32) (err error) {
	wireMsg, buf, err := pool.PackBuffer(v)
	if err != nil {
		return err
	}
	data := map[string]interface{}{}
	expire := time.Now().Unix() + int64(ttl)
	err = mapstructure.Decode(CacheData{
		Ttl:    ttl,
		Msg:    wireMsg,
		Expire: expire,
	}, &data)
	if err != nil {
		return err
	}
	defer pool.ReleaseBuf(buf)
	return r.client.HSet(ctx, key, data).Err()
}

// Close closes the redis client.
func (r *redisCache) Close() error {
	return r.client.Close()
}
