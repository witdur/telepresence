package daemon

import (
	"context"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	dns2 "github.com/miekg/dns"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/datawire/dlib/dlog"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/daemon/dns"
	"github.com/telepresenceio/telepresence/v2/pkg/iputil"
)

const kubernetesZone = "cluster.local"

type dnsKey struct {
	query string
	qType uint16
}

// outbound does stuff, idk, I didn't write it.
//
// A zero outbound is invalid; you must use newOutbound.
type outbound struct {
	dnsListener net.PacketConn
	noSearch    bool
	router      *tunRouter

	// Namespaces, accessible using <service-name>.<namespace-name>
	namespaces map[string]struct{}
	domains    map[string]struct{}
	search     []string

	// dnsConfigured is closed when the dnsWorker has configured
	// the dnsServer.
	dnsConfigured chan struct{}

	kubeDNS chan net.IP

	// The domainsLock locks usage of namespaces, domains, and search
	domainsLock sync.RWMutex

	// Lock preventing concurrent calls to setSearchPath
	searchPathLock sync.Mutex

	setSearchPathFunc func(c context.Context, paths []string)

	work chan func(context.Context) error

	// dnsRecursion unique set of DNS queries currently in progress.
	dnsRecursion     map[dnsKey]*int
	dnsRecursionLock sync.Mutex

	dnsConfig *rpc.DNSConfig
}

// splitToUDPAddr splits the given address into an UDPAddr. It's
// an  error if the address is based on a hostname rather than an IP.
func splitToUDPAddr(netAddr net.Addr) (*net.UDPAddr, error) {
	ip, port, err := iputil.SplitToIPPort(netAddr)
	if err != nil {
		return nil, err
	}
	return &net.UDPAddr{IP: ip, Port: int(port)}, nil
}

// newOutbound returns a new properly initialized outbound object.
//
// If dnsIP is empty, it will be detected from /etc/resolv.conf
func newOutbound(c context.Context, dnsIPStr string, noSearch bool) (*outbound, error) {
	lc := &net.ListenConfig{}
	listener, err := lc.ListenPacket(c, "udp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	// seed random generator (used when shuffling IPs)
	rand.Seed(time.Now().UnixNano())

	ret := &outbound{
		dnsListener: listener,
		dnsConfig: &rpc.DNSConfig{
			LocalIp: iputil.Parse(dnsIPStr),
		},
		noSearch:      noSearch,
		namespaces:    make(map[string]struct{}),
		domains:       make(map[string]struct{}),
		dnsRecursion:  make(map[dnsKey]*int),
		search:        []string{""},
		work:          make(chan func(context.Context) error),
		dnsConfigured: make(chan struct{}),
		kubeDNS:       make(chan net.IP, 1),
	}

	if ret.router, err = newTunRouter(); err != nil {
		return nil, err
	}
	return ret, nil
}

// routerServerWorker starts the TUN router and reads from the work queue of firewall config
// changes that is written to by the 'Update' gRPC call.
func (o *outbound) routerServerWorker(c context.Context) (err error) {
	go func() {
		// No need to select between <-o.work and <-c.Done(); o.work will get closed when we start
		// shutting down.
		for f := range o.work {
			if c.Err() == nil {
				// As long as we're not shutting down, keep doing work.  (If we are shutting
				// down, do nothing but don't 'break'; keep draining the queue.)
				if err = f(c); err != nil {
					dlog.Error(c, err)
				}
			}
		}
	}()
	return o.router.run(c)
}

// On a macOS, Docker uses its own search-path for single label names. This means that the search path that is declared
// in the macOS resolver is ignored although the rest of the DNS-resolution works OK. Since the search-path is likely to
// change during a session, a stable fake domain is needed to emulate the search-path. That fake-domain can then be used
// in the search path declared in the Docker config. The "tel2-search" domain fills this purpose and a request for
// "<single label name>.tel2-search." will be resolved as "<single label name>." using the search path of this resolver.
const tel2SubDomain = "tel2-search"
const tel2SubDomainDot = tel2SubDomain + "."
const dotKubernetesZone = "." + kubernetesZone + "."

var localhostIPv6 = []net.IP{{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}}
var localhostIPv4 = []net.IP{{127, 0, 0, 1}}

func (o *outbound) shouldDoClusterLookup(query string) bool {
	if strings.HasSuffix(query, dotKubernetesZone) && strings.Count(query, ".") < 4 {
		return false
	}

	query = query[:len(query)-1] // skip last dot

	// Always include configured includeSuffixes
	for _, sfx := range o.dnsConfig.IncludeSuffixes {
		if strings.HasSuffix(query, sfx) {
			return true
		}
	}

	// Skip configured excludeSuffixes
	for _, sfx := range o.dnsConfig.ExcludeSuffixes {
		if strings.HasSuffix(query, sfx) {
			return false
		}
	}
	return true
}

// The maximum number of queries for the exact same name during resolution of that name
// before the resolveInCluster() gives up and returns nil. This is a best effort since
// there's no way to know if the query actually is a recursion or if it's simultaneously
// made by several clients at the same time.
const maxRecursion = 3

func (o *outbound) resolveInCluster(c context.Context, qType uint16, query string) []net.IP {
	query = strings.ToLower(query)
	query = strings.TrimSuffix(query, tel2SubDomainDot)

	if query == "localhost." {
		// BUG(lukeshu): I have no idea why a lookup
		// for localhost even makes it to here on my
		// home WiFi when connecting to a k3sctl
		// cluster (but not a kubernaut.io cluster).
		// But it does, so I need this in order to be
		// productive at home.  We should really
		// root-cause this, because it's weird.
		if qType == dns2.TypeAAAA {
			return localhostIPv6
		}
		return localhostIPv4
	}

	if !o.shouldDoClusterLookup(query) {
		return nil
	}

	key := dnsKey{query, qType}
	o.dnsRecursionLock.Lock()
	recursion := o.dnsRecursion[key]
	if recursion == nil {
		r := 1
		recursion = &r
		o.dnsRecursion[key] = recursion
	} else {
		if *recursion+1 >= maxRecursion {
			o.dnsRecursionLock.Unlock()
			return nil
		}
		*recursion++
	}
	o.dnsRecursionLock.Unlock()

	// Give the cluster lookup a reasonable timeout.
	c, cancel := context.WithTimeout(c, o.dnsConfig.LookupTimeout.AsDuration())
	defer func() {
		cancel()
		o.dnsRecursionLock.Lock()
		*recursion--
		if *recursion == 0 {
			delete(o.dnsRecursion, key)
		}
		o.dnsRecursionLock.Unlock()
	}()

	queryWithNoTrailingDot := query[:len(query)-1]
	dlog.Debugf(c, "LookupHost %q", queryWithNoTrailingDot)
	response, err := o.router.managerClient.LookupHost(c, &manager.LookupHostRequest{
		Session: o.router.session,
		Host:    queryWithNoTrailingDot,
	})
	if err != nil {
		dlog.Error(c, client.CheckTimeout(c, err))
		return nil
	}
	if len(response.Ips) == 0 {
		return nil
	}
	ips := make(iputil.IPs, len(response.Ips))
	for i, ip := range response.Ips {
		ips[i] = ip
	}
	return ips
}

func (o *outbound) setInfo(ctx context.Context, info *rpc.OutboundInfo) error {
	if info.Dns == nil {
		info.Dns = &rpc.DNSConfig{}
	}
	if oldIP := o.dnsConfig.GetLocalIp(); len(oldIP) > 0 {
		info.Dns.LocalIp = oldIP
	}
	if len(info.Dns.ExcludeSuffixes) == 0 {
		info.Dns.ExcludeSuffixes = []string{
			".arpa",
			".com",
			".io",
			".net",
			".org",
			".ru",
		}
	}
	if info.Dns.LookupTimeout.AsDuration() <= 0 {
		info.Dns.LookupTimeout = durationpb.New(4 * time.Second)
	}
	o.dnsConfig = info.Dns

	kubeDNS := o.kubeDNS
	if len(info.Dns.RemoteIp) > 0 {
		// Never mind what the traffic-manager reports
		kubeDNS = nil
		go func() {
			select {
			case <-ctx.Done():
				return
			case o.kubeDNS <- info.Dns.RemoteIp:
			}
		}()
	}
	return o.router.setOutboundInfo(ctx, info, kubeDNS)
}

func (o *outbound) getInfo() *rpc.OutboundInfo {
	info := rpc.OutboundInfo{
		Dns: o.dnsConfig,
	}

	if len(o.router.alsoProxySubnets) > 0 {
		info.AlsoProxySubnets = make([]*manager.IPNet, len(o.router.alsoProxySubnets))
		for i, ap := range o.router.alsoProxySubnets {
			info.AlsoProxySubnets[i] = iputil.IPNetToRPC(ap)
		}
	}

	return &info
}

func (o *outbound) noMoreUpdates() {
	close(o.work)
}

// SetSearchPath updates the DNS search path used by the resolver
func (o *outbound) setSearchPath(c context.Context, paths []string) {
	select {
	case <-c.Done():
	case <-o.dnsConfigured:
		o.searchPathLock.Lock()
		defer o.searchPathLock.Unlock()
		o.setSearchPathFunc(c, paths)
		dns.Flush(c)
	}
}
