package check_network

import (
	"context"
	"net"
	"time"

	"github.com/IrineSistiana/mosdns/dispatcher/handler"
)

const PluginType = "check_network"

func init() {
	handler.RegInitFunc(PluginType, Init, func() interface{} { return new(Args) })
}

var _ handler.MatcherPlugin = (*checker)(nil)

type Args struct {
	Timeout uint   `yaml:"timeout"`
	Network string `yaml:"network"`
	Address string `yaml:"address"`
}

type checker struct {
	*handler.BP
	args *Args
}

func (c *checker) Match(ctx context.Context, qCtx *handler.Context) (matched bool, err error) {
	timeout := time.Duration(c.args.Timeout * uint(time.Millisecond))
	con, err := net.DialTimeout(c.args.Network, c.args.Address, timeout)
	if err == nil {
		con.Close()
		return true, nil
	} else {
		return false, nil
	}
}

func Init(bp *handler.BP, args interface{}) (p handler.Plugin, err error) {
	return &checker{
		BP:   bp,
		args: args.(*Args),
	}, nil
}
