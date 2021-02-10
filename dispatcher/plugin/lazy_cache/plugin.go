package lazy_cache

import (
	"context"
	"fmt"
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
var _ handler.ContextPlugin = (*cachePlugin)(nil)

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
	get(ctx context.Context, key string) (v *dns.Msg, ttl uint32, expire int64, ok bool, err error)
	// store stores the v into cache. It stores a copy of v.
	store(ctx context.Context, key string, v *dns.Msg, ttl uint32) (err error)
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
	key, cacheHit := c.searchAndReply(ctx, qCtx)
	if cacheHit {
		return true, nil
	}

	if len(key) != 0 {
		de := newDeferStore(key, c)
		qCtx.DeferExec(de)
	}

	return false, nil
}

func (c *cachePlugin) searchAndReply(ctx context.Context, qCtx *handler.Context) (key string, cacheHit bool) {
	q := qCtx.Q()
	key, err := utils.GetMsgKey(q, 0)
	if err != nil {
		c.L().Warn("unable to get msg key, skip it", qCtx.InfoField(), zap.Error(err))
		return "", false
	}

	r, rawTtl, expire, isOk, err := c.c.get(ctx, key)
	if isOk == false {
		if err != nil {
			c.L().Warn("unable to access cache, skip it", qCtx.InfoField(), zap.Error(err))
		}
		return key, false
	}

	if r != nil { // if cache hit
		c.L().Debug("cache hit", qCtx.InfoField())
		r.Id = q.Id
		ttl := expire - time.Now().Unix()
		if ttl > 0 {
			dnsutils.SetTTL(r, uint32(ttl))
		} else {
			if ttl*-1 > int64(maxExpire) {
				c.L().Debug("cache expire and exceeds "+fmt.Sprint(maxExpire)+"s", qCtx.InfoField())
				return key, false
			}
			dnsutils.SetTTL(r, uint32(1))
			c.L().Debug("cache expire", qCtx.InfoField(), zap.Uint32("rawTtl", rawTtl))
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
		return key, true
	}
	return key, false
}

type deferCacheStore struct {
	key string
	p   *cachePlugin
}

func newDeferStore(key string, p *cachePlugin) *deferCacheStore {
	return &deferCacheStore{key: key, p: p}
}

// Exec caches the response.
// It never returns an err. Because a cache fault should not terminate the query process.
func (d *deferCacheStore) Exec(ctx context.Context, qCtx *handler.Context) (err error) {
	if err := d.exec(ctx, qCtx); err != nil {
		d.p.L().Warn("failed to cache the data", qCtx.InfoField(), zap.Error(err))
	}
	return nil
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

func (d *deferCacheStore) exec(ctx context.Context, qCtx *handler.Context) (err error) {
	return runStore(ctx, d.p.c, qCtx.R(), d.key)
}

func (c *cachePlugin) Connect(ctx context.Context, qCtx *handler.Context, pipeCtx *handler.PipeContext) (err error) {
	key, cacheHit := c.searchAndReply(ctx, qCtx)
	if cacheHit {
		return nil
	}

	err = pipeCtx.ExecNextPlugin(ctx, qCtx)
	if err != nil {
		return err
	}

	if len(key) != 0 {
		_ = newDeferStore(key, c).Exec(ctx, qCtx)
	}

	return nil
}
