package lazy_cache

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/IrineSistiana/mosdns/dispatcher/handler"
	"github.com/IrineSistiana/mosdns/dispatcher/pkg/dnsutils"
	"github.com/IrineSistiana/mosdns/dispatcher/pkg/utils"
	"github.com/miekg/dns"
	"go.uber.org/zap"
)

const (
	PluginType        = "lazy_cache"
	maxTTL     uint32 = 3600 * 24 * 7 // one week
	maxExpire  uint32 = 3600 * 24
)

func init() {
	handler.RegInitFunc(PluginType, Init, func() interface{} { return new(Args) })
}

var _ handler.ESExecutablePlugin = (*cachePlugin)(nil)

type Args struct {
	Size           int    `yaml:"size"`
	Redis          string `yaml:"redis"`
	UpdateSequence string `yaml:"update_sequence"`
}

type cachePlugin struct {
	*handler.BP
	args *Args
	c    dnsCache
}

type dnsCache interface {
	// get retrieves v from cache. The returned v is a copy of the original msg
	// that stored in the cache.
	get(ctx context.Context, key string) (v *dns.Msg, ttl uint32, expire int64, err error)
	// store stores the v into cache. It stores a copy of v.
	store(ctx context.Context, key string, v *dns.Msg, ttl uint32) (err error)
	// Closer closes the cache backend.
	io.Closer
}

type CacheData struct {
	Expire int64  `mapstructure:"expire"`
	Ttl    uint32 `mapstructure:"ttl"`
	Msg    []byte `mapstructure:"msg"`
}

func Init(bp *handler.BP, args interface{}) (p handler.Plugin, err error) {
	return newCachePlugin(bp, args.(*Args))
}

func newCachePlugin(bp *handler.BP, args *Args) (*cachePlugin, error) {
	var c dnsCache
	var err error
	if len(args.Redis) != 0 {
		c, err = newRedisCache(args.Redis)
		if err != nil {
			return nil, err
		}
	} else {
		panic("redis arg is not supplied")
	}
	return &cachePlugin{
		BP:   bp,
		args: args,
		c:    c,
	}, nil
}

// ExecES searches the cache. If cache hits, earlyStop will be true.
// It never returns an err. Because a cache fault should not terminate the query process.
func (c *cachePlugin) ExecES(ctx context.Context, qCtx *handler.Context) (earlyStop bool, err error) {
	q := qCtx.Q()
	key, err := utils.GetMsgKey(q, 0)
	if err != nil {
		c.L().Warn("unable to get msg key", qCtx.InfoField(), zap.Error(err))
		return false, nil
	}

	// lookup in cache
	r, rawTTL, expire, err := c.c.get(ctx, key)
	if err != nil {
		c.L().Warn("unable to access cache", qCtx.InfoField(), zap.Error(err))
		return false, nil
	}

	// cache hit
	if r != nil {
		r.Id = q.Id
		c.L().Debug("cache hit", qCtx.InfoField())
		ttl := expire - time.Now().Unix()
		if ttl > 0 {
			dnsutils.SetTTL(r, uint32(ttl))
		} else {
			if ttl*-1 > int64(maxExpire) {
				c.L().Debug("cache expired and exceeds "+fmt.Sprint(maxExpire)+"s", qCtx.InfoField())
				qCtx.DeferExec(newDeferStore(key, c.c))
				return false, nil
			}
			dnsutils.SetTTL(r, uint32(0))
			c.L().Debug("cache expired", qCtx.InfoField(), zap.Uint32("rawTTL", rawTTL))
			go func(ctx context.Context, qCtx *handler.Context) error {
				p, err := handler.GetPlugin(c.args.UpdateSequence)
				defer func() {
					if err != nil {
						c.L().Warn("failed to update cache", qCtx.InfoField(), zap.Error(err))
					}
				}()
				if err != nil {
					return err
				}
				_, err = p.ExecES(ctx, qCtx)
				if err != nil {
					return err
				}
				err = runStore(ctx, c.c, qCtx.R(), key)
				return err
			}(context.Background(), qCtx.Copy())
		}
		qCtx.SetResponse(r, handler.ContextStatusResponded)
		return true, nil
	}

	// cache miss
	qCtx.DeferExec(newDeferStore(key, c.c))
	return false, nil
}

type deferCacheStore struct {
	key     string
	backend dnsCache
}

func newDeferStore(key string, backend dnsCache) *deferCacheStore {
	return &deferCacheStore{key: key, backend: backend}
}

// Exec caches the response.
// It never returns an err. Because a cache fault should not terminate the query process.
func (d *deferCacheStore) Exec(ctx context.Context, qCtx *handler.Context) (err error) {
	return runStore(ctx, d.backend, qCtx.R(), d.key)
}

func runStore(ctx context.Context, c dnsCache, r *dns.Msg, k string) (err error) {
	if r != nil && r.Rcode == dns.RcodeSuccess && r.Truncated == false && len(r.Answer) != 0 {
		ttl := dnsutils.GetMinimalTTL(r)
		if ttl > maxTTL {
			ttl = maxTTL
		}
		return c.store(ctx, k, r, ttl)
	}
	return nil
}
