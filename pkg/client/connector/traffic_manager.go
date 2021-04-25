package connector

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/user"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/datawire/dlib/dexec"
	"github.com/datawire/dlib/dtime"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/datawire/ambassador/pkg/kates"
	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/connector"
	"github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/actions"
	"github.com/telepresenceio/telepresence/v2/pkg/iputil"
)

// trafficManager is a handle to access the Traffic Manager in a
// cluster.
type trafficManager struct {
	*installer // installer is also a k8sCluster
	getAPIKey  func(context.Context, string, bool) (string, error)

	// local information
	env         client.Env
	installID   string // telepresence's install ID
	userAndHost string // "laptop-username@laptop-hostname"

	// manager client
	managerClient manager.ManagerClient
	managerErr    error     // if managerClient is nil, why it's nil
	startup       chan bool // gets closed when managerClient is fully initialized (or managerErr is set)

	sessionInfo *manager.SessionInfo // sessionInfo returned by the traffic-manager

	// grpcPort is the local TCP port number that gets forwarded to the port that where the
	// manager pod exposes the gRPC API.
	grpcPort int32

	// Map of desired mount points for intercepts
	mountPoints sync.Map

	// outboundInfo is the information that is sent to the daemon when a new connection to the
	// traffic-manager gRPC has been initially established or when a reconnect has been made.
	outboundInfo *daemon.OutboundInfo
}

// newTrafficManager returns a TrafficManager resource for the given
// cluster if it has a Traffic Manager service.
func newTrafficManager(
	_ context.Context,
	env client.Env,
	cluster *k8sCluster,
	installID string,
	getAPIKey func(context.Context, string, bool) (string, error),
) (*trafficManager, error) {
	userinfo, err := user.Current()
	if err != nil {
		return nil, errors.Wrap(err, "user.Current()")
	}
	host, err := os.Hostname()
	if err != nil {
		return nil, errors.Wrap(err, "os.Hostname()")
	}

	// Ensure that we have a traffic-manager to talk to.
	ti, err := newTrafficManagerInstaller(cluster)
	if err != nil {
		return nil, errors.Wrap(err, "new installer")
	}
	tm := &trafficManager{
		installer:   ti,
		env:         env,
		installID:   installID,
		startup:     make(chan bool),
		userAndHost: fmt.Sprintf("%s@%s", userinfo.Username, host),
		getAPIKey:   getAPIKey,
	}

	return tm, nil
}

func (tm *trafficManager) waitUntilStarted(c context.Context) error {
	select {
	case <-c.Done():
		return client.CheckTimeout(c, &client.GetConfig(c).Timeouts.TrafficManagerConnect, nil)
	case <-tm.startup:
		return tm.managerErr
	}
}

func (tm *trafficManager) run(c context.Context) error {
	err := tm.ensureManager(c, tm.env)
	if err != nil {
		tm.managerErr = err
		close(tm.startup)
		return err
	}
	return client.Retry(c, "svc/traffic-manager port-forward", tm.portForward, 2*time.Second, 6*time.Second)
}

var pfErrRx = regexp.MustCompile(`\AE\d.*]\s*(.*)\z`)

// portForward starts a kubectl port-forward command and scans its output for a "Forwarding from" message.
// When it arrives, the port is used in a call to trafficManager.connectGrpc() of the traffic manager and
// if that succeeds, it is followed by a new call to the trafficManager.innerRun() function.
func (tm *trafficManager) portForward(c context.Context) error {
	var portArg string
	if tm.grpcPort > 0 {
		portArg = fmt.Sprintf("%d:%d", tm.grpcPort, ManagerPortHTTP)
	} else {
		portArg = fmt.Sprintf(":%d", ManagerPortHTTP)
	}
	args := make([]string, 0, len(tm.flagArgs)+5)
	args = append(args, tm.flagArgs...)
	args = append(args, "port-forward", "--namespace", managerNamespace, "svc/traffic-manager", portArg)

	pfCtx, pfCancel := context.WithCancel(c)
	defer pfCancel()

	pf := dexec.CommandContext(pfCtx, "kubectl", args...)
	pfOut, err := pf.StdoutPipe()
	if err != nil {
		return err
	}
	defer pfOut.Close()
	pfErr, err := pf.StderrPipe()
	if err != nil {
		return err
	}
	defer pfErr.Close()

	// We want this command to keep on running. If it returns an error, then it was unsuccessful.
	if err = pf.Start(); err != nil {
		pfOut.Close()
		dlog.Errorf(c, "port-forward failed to start: %v", client.RunError(err))
		return err
	}

	// Give port-forward at least the port-forward timeout to produce the correct output and spawn the next process.
	// The timer is stopped as soon as the port-forward succeeds
	toc := client.GetConfig(c).Timeouts
	timer := time.AfterFunc(toc.TrafficManagerConnect, func() {
		pfCancel()
	})

	grpcPortCh := make(chan int)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		sc := bufio.NewScanner(pfOut)
		rxPortForward := regexp.MustCompile(`\AForwarding from \d+\.\d+\.\d+\.\d+:(\d+) -> (\d+)`)
		for sc.Scan() {
			if rxr := rxPortForward.FindStringSubmatch(sc.Text()); rxr != nil {
				fromPort, _ := strconv.Atoi(rxr[1])
				toPort, _ := strconv.Atoi(rxr[2])
				if toPort == ManagerPortHTTP {
					// A retry must use the same port
					grpcPortCh <- fromPort
					for sc.Scan() {
						// Consume and toss further output
					}
					break
				}
			}
		}
	}()

	// An error channel receives all errors until pfc is done or errCh closes
	errCh := make(chan error)
	var errs []error
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-pfCtx.Done():
				return
			case err := <-errCh:
				errs = append(errs, err)
			}
		}
	}()

	// kubectl port-forward exits with zero although errors are caught during the run
	// so those errors must be captured from stderr and dealt with.
	wg.Add(1)
	go func() {
		defer wg.Done()
		sc := bufio.NewScanner(pfErr)
		for sc.Scan() {
			txt := strings.TrimSpace(sc.Text())
			if errTxt := pfErrRx.FindStringSubmatch(txt); errTxt != nil {
				errCh <- errors.New(errTxt[1])
				close(grpcPortCh)
				pfCancel()
				break
			}
		}
	}()

	// wait for the grpcPort to arrive. When it does, make an attempt to connect to the
	// gRPC API. If it succeeds, stop the timer and call trafficManager.innerRun()
	wg.Add(1)
	go func() {
		defer wg.Done()
		grpcPort := <-grpcPortCh
		if grpcPort == 0 {
			return
		}

		// Retry a call to connect the gRPC. It shouldn't fail but the port forward isn't
		// always complete although the output seems to indicate that it is. So one retry is
		// motivated.
		tos := &client.GetConfig(c).Timeouts
		// An initial sleep of at least one second is needed for the port forward to become
		// effective. Without this, the first connect attempt will always fail.
		dtime.SleepWithContext(pfCtx, time.Second)

		var err error
		for retry := 0; ; {
			tc, tCancel := context.WithTimeout(pfCtx, toc.TrafficManagerAPI)
			err = tm.connectGrpc(tc, grpcPort)
			tCancel()
			if err == nil {
				timer.Stop()
				break
			}
			if pfCtx.Err() != nil {
				// Parent cancelled, nothing left to do then.
				err = nil
				return
			}
			retry++
			if retry == 2 {
				errCh <- client.CheckTimeout(tc, &tos.TrafficManagerAPI, err)
				return
			}
		}

		go func() {
			// Call start unless already started (which will be the case when this restart is due to
			// lossy connection).
			if tm.startup != nil {
				err := tm.innerRun(c)
				pfCancel()
				if err != nil {
					dlog.Error(c, err)
				}
			} else {
				// Tell the daemon that a reconnect was made. It must reset it's manager client.
				dlog.Debug(c, "Sending new outbound info to daemon")
				if _, err := tm.daemon.SetOutboundInfo(c, tm.outboundInfo); err != nil {
					dlog.Error(c, err)
				}
			}
		}()
	}()

	// let the port forward continue running. It will either be killed by the
	// timer (if it didn't produce the expected output) or by a context cancel.
	if err = pf.Wait(); err != nil {
		if pfCtx.Err() == nil {
			errCh <- err
		}
	}
	pfCancel()
	wg.Wait()

	switch len(errs) {
	case 0:
	case 1:
		err = errs[0]
	default:
		sb := strings.Builder{}
		sb.WriteString(errs[0].Error())
		for _, err = range errs[1:] {
			sb.WriteString(", ")
			sb.WriteString(err.Error())
		}
		err = errors.New(sb.String())
	}
	return err
}

func (tm *trafficManager) connectGrpc(c context.Context, grpcPort int) error {
	tm.grpcPort = int32(grpcPort)

	// First check. Establish connection
	conn, err := grpc.DialContext(c, fmt.Sprintf("127.0.0.1:%d", grpcPort),
		grpc.WithInsecure(),
		grpc.WithNoProxy(),
		grpc.WithBlock())
	if err != nil {
		return err
	}

	mClient := manager.NewManagerClient(conn)
	if tm.sessionInfo == nil {
		tm.sessionInfo, err = mClient.ArriveAsClient(c, &manager.ClientInfo{
			Name:      tm.userAndHost,
			InstallId: tm.installID,
			Product:   "telepresence",
			Version:   client.Version(),
			ApiKey:    func() string { tok, _ := tm.getAPIKey(c, "manager", false); return tok }(),
		})
		if err != nil {
			conn.Close()
			return err
		}
	}
	tm.managerClient = mClient
	return nil
}

func (tm *trafficManager) innerRun(c context.Context) (err error) {
	var conn *grpc.ClientConn
	defer func() {
		if err != nil {
			dlog.Error(c, err)
			tm.managerErr = err
			if conn != nil {
				conn.Close()
			}
			if tm.startup != nil {
				close(tm.startup)
				tm.startup = nil
			}
		}
	}()

	// Tell daemon what it needs to know in order to establish outbound traffic to the cluster
	tm.outboundInfo, err = tm.getOutboundInfo(c)
	if err != nil {
		return err
	}
	if _, err = tm.daemon.SetOutboundInfo(c, tm.outboundInfo); err != nil {
		return err
	}

	g := dgroup.NewGroup(c, dgroup.GroupConfig{})
	g.Go("remain", tm.remain)
	g.Go("intercept-port-forward", tm.workerPortForwardIntercepts)
	close(tm.startup)
	tm.startup = nil
	return g.Wait()
}

func (tm *trafficManager) session() *manager.SessionInfo {
	return tm.sessionInfo
}

// hasOwner parses an object and determines whether the object has an
// owner that is of a kind we prefer. Currently the only owner that we
// prefer is a Deployment, but this may grow in the future
func (tm *trafficManager) hasOwner(obj kates.Object) bool {
	for _, owner := range obj.GetOwnerReferences() {
		if owner.Kind == "Deployment" {
			return true
		}
	}
	return false
}

// getObjectAndLabels gets the object + its associated labels, as well as a reason
// it cannot be intercepted if that is the case.
func (tm *trafficManager) getObjectAndLabels(ctx context.Context, objectKind, namespace, name string) (kates.Object, map[string]string, string, error) {
	var object kates.Object
	var labels map[string]string
	var reason string
	switch objectKind {
	case "Deployment":
		dep, err := tm.findDeployment(ctx, namespace, name)
		if err != nil {
			// Removed from snapshot since the name slice was obtained
			if !errors2.IsNotFound(err) {
				dlog.Error(ctx, err)
			}

			return nil, nil, "", err
		}

		if dep.Status.Replicas == int32(0) {
			reason = "Has 0 replicas"
		}
		object = dep
		labels = dep.Spec.Template.Labels

	case "ReplicaSet":
		rs, err := tm.findReplicaSet(ctx, namespace, name)
		if err != nil {
			// Removed from snapshot since the name slice was obtained
			if !errors2.IsNotFound(err) {
				dlog.Error(ctx, err)
			}
			return nil, nil, "", err
		}

		if rs.Status.Replicas == int32(0) {
			reason = "Has 0 replicas"
		}
		object = rs
		labels = rs.Spec.Template.Labels

	case "StatefulSet":
		statefulSet, err := tm.findStatefulSet(ctx, namespace, name)
		if err != nil {
			// Removed from snapshot since the name slice was obtained
			if !errors2.IsNotFound(err) {
				dlog.Error(ctx, err)
			}
			return nil, nil, "", err
		}

		if statefulSet.Status.Replicas == int32(0) {
			reason = "Has 0 replicas"
		}
		object = statefulSet
		labels = statefulSet.Spec.Template.Labels
	default:
		reason = "No workload telepresence knows how to intercept"
	}
	return object, labels, reason, nil
}

// getInfosForWorkload creates a WorkloadInfo for every workload in names
// of the given objectKind.  Additionally, it uses information about the
// filter param, which is configurable, to decide which workloads to add
// or ignore based on the filter criteria.
func (tm *trafficManager) getInfosForWorkload(
	ctx context.Context,
	names []string,
	objectKind,
	namespace string,
	iMap map[string]*manager.InterceptInfo,
	aMap map[string]*manager.AgentInfo,
	filter rpc.ListRequest_Filter,
) []*rpc.WorkloadInfo {
	workloadInfos := make([]*rpc.WorkloadInfo, 0)
	for _, name := range names {
		iCept, ok := iMap[name]
		if !ok && filter <= rpc.ListRequest_INTERCEPTS {
			continue
		}
		agent, ok := aMap[name]
		if !ok && filter <= rpc.ListRequest_INSTALLED_AGENTS {
			continue
		}
		reason := ""
		if agent == nil && iCept == nil {
			object, labels, reason, err := tm.getObjectAndLabels(ctx, objectKind, namespace, name)
			if err != nil {
				continue
			}
			if reason == "" {
				// If an object is owned by a higher level workload, then users should
				// intercept that workload so we will not include it in our slice.
				if tm.hasOwner(object) {
					dlog.Infof(ctx, "Not including snapshot for object as it has an owner: %s.%s", object.GetName(), object.GetNamespace())
					continue
				}

				matchingSvcs, err := tm.findMatchingServices(ctx, "", "", namespace, labels)
				if err != nil {
					continue
				}
				if len(matchingSvcs) == 0 {
					reason = "No service with matching selector"
				}
			}

			// If we have a reason, that means it's not interceptable, so we only
			// pass the workload through if they want to see all workloads, not
			// just the interceptable ones
			if !ok && filter <= rpc.ListRequest_INTERCEPTABLE && reason != "" {
				continue
			}
		}

		workloadInfos = append(workloadInfos, &rpc.WorkloadInfo{
			Name:                   name,
			NotInterceptableReason: reason,
			AgentInfo:              agent,
			InterceptInfo:          iCept,
			WorkloadResourceType:   objectKind,
		})
	}
	return workloadInfos
}

func (tm *trafficManager) workloadInfoSnapshot(ctx context.Context, rq *rpc.ListRequest) *rpc.WorkloadInfoSnapshot {
	var iMap map[string]*manager.InterceptInfo

	namespace := tm.actualNamespace(rq.Namespace)
	if namespace == "" {
		// namespace is not currently mapped
		return &rpc.WorkloadInfoSnapshot{}
	}

	if is, _ := actions.ListMyIntercepts(ctx, tm.managerClient, tm.session().SessionId); is != nil {
		iMap = make(map[string]*manager.InterceptInfo, len(is))
		for _, i := range is {
			if i.Spec.Namespace == namespace {
				iMap[i.Spec.Agent] = i
			}
		}
	} else {
		iMap = map[string]*manager.InterceptInfo{}
	}
	var aMap map[string]*manager.AgentInfo
	if as, _ := actions.ListAllAgents(ctx, tm.managerClient, tm.session().SessionId); as != nil {
		aMap = make(map[string]*manager.AgentInfo, len(as))
		for _, a := range as {
			if a.Namespace == namespace {
				aMap[a.Name] = a
			}
		}
	} else {
		aMap = map[string]*manager.AgentInfo{}
	}

	filter := rq.Filter
	workloadInfos := make([]*rpc.WorkloadInfo, 0)

	// These are all the workloads we care about and their associated function
	// to get the names of those workloads
	workloadsToGet := map[string]func(context.Context, string) ([]string, error){
		"Deployment":  tm.deploymentNames,
		"ReplicaSet":  tm.replicaSetNames,
		"StatefulSet": tm.statefulSetNames,
	}

	for workloadKind, namesFunc := range workloadsToGet {
		workloadNames, err := namesFunc(ctx, namespace)
		if err != nil {
			dlog.Error(ctx, err)
			dlog.Infof(ctx, "Skipping getting info for workloads: %s", workloadKind)
			continue
		}
		newWorkloadInfos := tm.getInfosForWorkload(ctx, workloadNames, workloadKind, namespace, iMap, aMap, filter)
		workloadInfos = append(workloadInfos, newWorkloadInfos...)
	}

	for localIntercept, localNs := range tm.localIntercepts {
		if localNs == namespace {
			workloadInfos = append(workloadInfos, &rpc.WorkloadInfo{InterceptInfo: &manager.InterceptInfo{
				Spec:              &manager.InterceptSpec{Name: localIntercept, Namespace: localNs},
				Disposition:       manager.InterceptDispositionType_ACTIVE,
				MechanismArgsDesc: "as local-only",
			}})
		}
	}

	return &rpc.WorkloadInfoSnapshot{Workloads: workloadInfos}
}

func (tm *trafficManager) remain(c context.Context) error {
	defer func() {
		tm.managerClient = nil
	}()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.Done():
			_ = tm.clearIntercepts(context.Background())
			_, _ = tm.managerClient.Depart(context.Background(), tm.session())
			return nil
		case <-ticker.C:
			_, err := tm.managerClient.Remain(c, &manager.RemainRequest{
				Session: tm.session(),
				ApiKey:  func() string { tok, _ := tm.getAPIKey(c, "manager", false); return tok }(),
			})
			if err != nil {
				if c.Err() == nil {
					dlog.Error(c, err)
					continue
				}
				return nil
			}
		}
	}
}

func (tm *trafficManager) setStatus(ctx context.Context, r *rpc.ConnectInfo) {
	if tm == nil {
		return
	}
	if tm.managerClient == nil {
		r.BridgeOk = false
		r.Intercepts = &manager.InterceptInfoSnapshot{}
		r.Agents = &manager.AgentInfoSnapshot{}
		if err := tm.managerErr; err != nil {
			r.ErrorText = err.Error()
		}
	} else {
		agents, _ := actions.ListAllAgents(ctx, tm.managerClient, tm.session().SessionId)
		intercepts, _ := actions.ListMyIntercepts(ctx, tm.managerClient, tm.session().SessionId)
		r.Agents = &manager.AgentInfoSnapshot{Agents: agents}
		r.Intercepts = &manager.InterceptInfoSnapshot{Intercepts: intercepts}
		r.SessionInfo = tm.session()
		r.BridgeOk = true
	}
}

// Given a slice of AgentInfo, this returns another slice of agents with one
// agent per namespace, name pair.
func getRepresentativeAgents(_ context.Context, agents []*manager.AgentInfo) []*manager.AgentInfo {
	type workload struct {
		name, namespace string
	}
	workloads := map[workload]bool{}
	var representativeAgents []*manager.AgentInfo
	for _, agent := range agents {
		wk := workload{name: agent.Name, namespace: agent.Namespace}
		if !workloads[wk] {
			workloads[wk] = true
			representativeAgents = append(representativeAgents, agent)
		}
	}
	return representativeAgents
}

func (tm *trafficManager) uninstall(c context.Context, ur *rpc.UninstallRequest) (*rpc.UninstallResult, error) {
	result := &rpc.UninstallResult{}
	agents, _ := actions.ListAllAgents(c, tm.managerClient, tm.session().SessionId)

	// Since workloads can have more than one replica, we get a slice of agents
	// where the agent to workload mapping is 1-to-1.  This is important
	// because in the ALL_AGENTS or default case, we could edit the same
	// workload n times for n replicas, which could cause race conditions
	agents = getRepresentativeAgents(c, agents)

	_ = tm.clearIntercepts(c)
	switch ur.UninstallType {
	case rpc.UninstallRequest_UNSPECIFIED:
		return nil, errors.New("invalid uninstall request")
	case rpc.UninstallRequest_NAMED_AGENTS:
		var selectedAgents []*manager.AgentInfo
		for _, di := range ur.Agents {
			found := false
			namespace := tm.actualNamespace(ur.Namespace)
			if namespace != "" {
				for _, ai := range agents {
					if namespace == ai.Namespace && di == ai.Name {
						found = true
						selectedAgents = append(selectedAgents, ai)
						break
					}
				}
			}
			if !found {
				result.ErrorText = fmt.Sprintf("unable to find a workload named %s.%s with an agent installed", di, namespace)
			}
		}
		agents = selectedAgents
		fallthrough
	case rpc.UninstallRequest_ALL_AGENTS:
		if len(agents) > 0 {
			if err := tm.removeManagerAndAgents(c, true, agents); err != nil {
				result.ErrorText = err.Error()
			}
		}
	default:
		// Cancel all communication with the manager
		if err := tm.removeManagerAndAgents(c, false, agents); err != nil {
			result.ErrorText = err.Error()
		}
	}
	return result, nil
}

var svcCIDRrx = regexp.MustCompile(`range of valid IPs is (.*)$`)

// getClusterCIDRs finds the service CIDR and the pod CIDRs of all nodes in the cluster
func (tm *trafficManager) getOutboundInfo(c context.Context) (*daemon.OutboundInfo, error) {
	// Get all nodes
	var nodes []kates.Node
	var cidr *net.IPNet

	svc := kates.Service{
		TypeMeta: metav1.TypeMeta{
			Kind: "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kube-dns",
			Namespace: "kube-system",
		},
	}
	err := tm.client.Get(c, &svc, &svc)
	if err != nil {
		return nil, fmt.Errorf("failed to get kube-dns.kube-system service")
	}
	kubeDNS := iputil.Parse(svc.Spec.ClusterIP)

	// make an attempt to create a service with ClusterIP that is out of range and then
	// check the error message for the correct range as suggested tin the second answer here:
	//   https://stackoverflow.com/questions/44190607/how-do-you-find-the-cluster-service-cidr-of-a-kubernetes-cluster
	// This requires an additional permission to create a service, which we might not have
	svc = kates.Service{
		TypeMeta: metav1.TypeMeta{
			Kind: "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "t2-tst-dummy",
		},
		Spec: v1.ServiceSpec{
			Ports:     []kates.ServicePort{{Port: 443}},
			ClusterIP: "1.1.1.1",
		},
	}

	var serviceSubnet *daemon.IPNet
	if err = tm.client.Create(c, &svc, &svc); err != nil {
		if match := svcCIDRrx.FindStringSubmatch(err.Error()); match != nil {
			_, cidr, err = net.ParseCIDR(match[1])
			if err != nil {
				dlog.Errorf(c, "unable to parse service CIDR %q", match[1])
			} else {
				serviceSubnet = iputil.IPNetToRPC(cidr)
			}
		} else {
			dlog.Debugf(c, "Probably insufficient permissions to attempt dummy service creation in order to find service subnet: %v", err)
		}
	}

	if serviceSubnet == nil {
		// Using a "kubectl cluster-info dump" or scanning all services generates a lot of unwanted traffic
		// and would quite possibly also require elevated permissions, so instead, we derive the service subnet
		// from the kubeDNS IP. This is cheating but a cluster may only have one service subnet and the mask is
		// unlikely to cover less than half the bits.
		dlog.Debugf(c, "Deriving serviceSubnet from %s (the IP of kube-dns.kube-system)", kubeDNS)
		bits := len(kubeDNS) * 8
		ones := bits / 2
		mask := net.CIDRMask(ones, bits) // will yield a 16 bit mask on IPv4 and 64 bit mask on IPv6.
		serviceSubnet = &daemon.IPNet{Ip: kubeDNS.Mask(mask), Mask: int32(ones)}
	}

	if err = tm.client.List(c, kates.Query{Kind: "Node"}, &nodes); err != nil {
		return nil, fmt.Errorf("failed to get nodes: %v", err)
	}
	podCIDRs := make([]*daemon.IPNet, 0, len(nodes)+1)
	for i := range nodes {
		node := &nodes[i]
		for _, podCIDR := range node.Spec.PodCIDRs {
			_, cidr, err = net.ParseCIDR(podCIDR)
			if err != nil {
				dlog.Errorf(c, "unable to parse podCIDR %q in %s.%s", podCIDR, node.Name, node.Namespace)
				continue
			}
			podCIDRs = append(podCIDRs, iputil.IPNetToRPC(cidr))
		}
	}

	return &daemon.OutboundInfo{
		Session:       tm.sessionInfo,
		ManagerPort:   tm.grpcPort,
		KubeDnsIp:     kubeDNS,
		ServiceSubnet: serviceSubnet,
		PodSubnets:    podCIDRs,
	}, nil
}
