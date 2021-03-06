//     Copyright (C) 2020-2021, IrineSistiana
//
//     This file is part of mosdns.
//
//     mosdns is free software: you can redistribute it and/or modify
//     it under the terms of the GNU General Public License as published by
//     the Free Software Foundation, either version 3 of the License, or
//     (at your option) any later version.
//
//     mosdns is distributed in the hope that it will be useful,
//     but WITHOUT ANY WARRANTY; without even the implied warranty of
//     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//     GNU General Public License for more details.
//
//     You should have received a copy of the GNU General Public License
//     along with this program.  If not, see <https://www.gnu.org/licenses/>.

package pool

import (
	"fmt"
	"github.com/miekg/dns"
)

func PackBuffer(m *dns.Msg) (wire, buf []byte, err error) {
	l := m.Len()
	if l > dns.MaxMsgSize || l <= 0 {
		return nil, nil, fmt.Errorf("msg length %d is invalid", l)
	}
	buf = allocator.Get(l + 4) // m.PackBuffer(buf) needs one more bit than its length. Wired. We give it a little more.

	wire, err = m.PackBuffer(buf)
	if err != nil {
		allocator.Release(buf)
		return nil, nil, err
	}
	return wire, buf, nil
}
