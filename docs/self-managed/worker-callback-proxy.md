# Worker Callback Proxy

A function worker calls back to the `invocation-service` or `grpc-proxy` pod
that accepted the request. Workers in the same cluster as the control plane
reach that pod directly, so they do not need this proxy, and
[High Availability](./high-availability.md) scales both services without it.

Workers in another cluster cannot reach control-plane pod IPs. The worker
callback proxy is an Envoy deployment that accepts those callbacks and
forwards each one to the pod that holds the request.

## How callbacks reach a pod

- **HTTP functions:** invocation-service hands the worker the pod target
  `http://<dashed-pod-ip>.invocation.nvcf.svc.cluster.local:8443` and the proxy
  address `https://invocation.<workerDomain>:10084`. The worker opens TLS to
  the proxy on TCP 10084 and verifies its `*.<workerDomain>` certificate. That
  session ends at the proxy. Because the target scheme is `http`, the worker
  sends a forward-proxy request through the session. The proxy then opens its
  own TLS session to the pod's 8443 listener and verifies the pod certificate
  against the stack CA and the pod name. The worker never sees the pod
  certificate. The proxy forwards only addresses of that form, and only the
  worker callback path. It can read the callback, as it can for gRPC.
- **gRPC functions:** grpc-proxy serves HTTP/3 callbacks and hands the worker
  `https://<dashed-pod-ip>.<workerDomain>`. DNS resolves that name to the
  proxy. The worker verifies the proxy's `*.<workerDomain>` certificate, and
  the proxy re-encrypts the connection to the grpc-proxy pod on UDP 10084. It
  accepts only a grpc-proxy certificate for
  `*.grpc-headless.nvcf.svc.cluster.local`.

## Prerequisites

- OpenBao and cert-manager from the stack (`openbao.enabled` and
  `certManager.enabled`, both on by default). Enabling the proxy provisions the
  OpenBao service PKI and `ClusterIssuer/nvcf-openbao-pki`, which sign the
  proxy, grpc-proxy and invocation-service certificates. The LLM add-on is
  not required.
- An invocation-service image with worker TLS support. The stack turns on its
  8443 worker TLS listener (`invocation.workerTls`) when the proxy is enabled,
  unless `workerCallbackProxy.invocation.upstreamTls.enabled` is false.
- Function workers that send `http://` callback targets as forward-proxy
  requests (worker-utils 1.3.2 or later). When Envoy's frontend certificate is
  issued by the stack CA, HTTP callbacks also need worker-utils 1.4.0 or later,
  the first release that trusts `NVCF_PROXY_CA_PEM` on that path. The stack's
  `api` chart selects the worker-utils version.
- No split-plane worker URLs: leave
  `global.workerEndpoints.invocationServiceURL`,
  `global.workerEndpoints.grpcProxyWorkerConnectURL`,
  `grpcproxy.workerConnectBaseURL`, and
  `invocation.env.WORKER_STREAM_PROPERTIES__SELF_ADDRESS` empty. They name
  addresses that workers would use instead of the proxy, so the stack refuses
  to render when any of them is set while `workerCallbackProxy.enabled` is
  true.
- Access to the Envoy image. The chart pulls `docker.io/envoyproxy/envoy` by
  default. To use a mirror, set `workerCallbackProxy.image.registry` and
  `workerCallbackProxy.image.repository`.

## Enable the proxy

1. In the self-managed environment file, enable the proxy and expose it to the
   compute cluster:

   ```yaml
   workerCallbackProxy:
     enabled: true
     workerDomain: grpc-worker.example.com
     service:
       type: LoadBalancer
   ```

   Workers reach the invocation tunnel at
   `https://invocation.<workerDomain>:10084`, which the wildcard DNS record in
   the next step covers. Set `global.workerEndpoints.invocationProxyURL` only
   when workers use another name; it must be `https` and covered by the
   `*.<workerDomain>` certificate.

   Use a private load balancer where you can, and allow only the compute
   cluster's egress addresses through your load balancer or network controls.
   Both callback paths are TLS, but any client that reaches the Service can
   open one.

   `workerDomain` is not the ingress domain. The two serve different callers:

   | Setting | Names | Resolves to | Used by |
   | --- | --- | --- | --- |
   | `global.domain` (ingress domain) | `api.<domain>`, `invocation.<domain>`, the other Gateway API routes | The Gateway load balancer | Clients and tools outside the cluster |
   | `workerCallbackProxy.workerDomain` | `<dashed-pod-ip>.<workerDomain>` and `invocation.<workerDomain>` | The worker callback proxy Service | Function workers in the compute cluster |

   Do not set `workerDomain` to the ingress domain. A wildcard record on the
   ingress domain points at the Gateway, and `invocation.<global.domain>`
   already names a Gateway route, so it cannot also name the proxy. Pick a
   separate DNS name for which you control the zone. Only the compute cluster
   needs to resolve it, so a private zone is enough.

2. Make `*.<workerDomain>` resolve to the proxy Service from the compute
   cluster. See [Expose the proxy and its DNS name](#expose-the-proxy-and-its-dns-name).
   Workers resolve every `<dashed-pod-ip>.<workerDomain>` name, not only the
   base name.

   A single cluster does not need the proxy or this step, because workers there
   call the pods directly. Leave `workerCallbackProxy.enabled` false. If you
   enable the proxy anyway, for example to test it, the stack advertises proxy
   addresses, so the names must resolve or callbacks fail.

   On a local k3d setup with separate control-plane and compute clusters, run
   `make -C tools/ncp-local-cluster configure-worker-callback-dns WORKER_CALLBACK_DOMAIN=grpc-worker.example.com`.
   It adds a CoreDNS rewrite to `kube-system/coredns-custom` in the compute
   cluster, `COMPUTE_CLUSTER_NAME` (default `ncp-local-compute-1`), where
   workers resolve DNS, and asks before it switches your kubectl context there.
   The rewrite points at `worker-callback-proxy.nvcf.svc.cluster.local`, which
   the compute cluster does not have yet, and the local control plane does not
   expose the proxy. Until [#3257](https://github.com/dsx-ai-factory/nvcf/issues/3257)
   adds both, set `WORKER_CALLBACK_PROXY_SERVICE` to a name that the compute
   cluster resolves to the proxy and that you have exposed yourself.

3. Apply the stack: `HELMFILE_ENV=<environment-name> helmfile sync`.

## Expose the proxy and its DNS name

### What to expose

The proxy Service listens on two ports. Both must be reachable from the compute
cluster through the load balancer:

| Port | Protocol | Carries |
| --- | --- | --- |
| 10084 | TCP | HTTP callbacks (`invocation.<workerDomain>`) |
| 443 | UDP | gRPC callbacks over HTTP/3 (`<dashed-pod-ip>.<workerDomain>`) |

Set `workerCallbackProxy.service.type: LoadBalancer`, and add the annotations
your platform needs for a private load balancer through
`workerCallbackProxy.service.annotations`. If the platform selects the load
balancer implementation by class, set
`workerCallbackProxy.service.loadBalancerClass`. The stack passes the whole
`workerCallbackProxy.service` block to the chart.

Confirm that your load balancer type forwards both a TCP and a UDP listener from
one Service, and that the security groups or network controls allow both ports
from the compute cluster's egress addresses. A gRPC call that times out with `DEADLINE_EXCEEDED` while HTTP works
often means the UDP port is closed or the name does not reach the proxy.

On AWS, a load balancer health check that probes the UDP gRPC port over TCP
cannot succeed, because that port accepts only UDP. The AWS Load Balancer
Controller checks the traffic port by default. If the load balancer reports the
proxy targets unhealthy, point the health check at the TCP port instead, with
these entries in `workerCallbackProxy.service.annotations`. The port to name
depends on the load balancer's target type
(`service.beta.kubernetes.io/aws-load-balancer-nlb-target-type`):

| Target type | `aws-load-balancer-healthcheck-port` | Why |
| --- | --- | --- |
| `ip` | `"10084"` | The controller probes the pod port. |
| `instance` | `invocation-tcp` | The controller resolves the Service port name to its allocated node port. |

```yaml
workerCallbackProxy:
  service:
    annotations:
      service.beta.kubernetes.io/aws-load-balancer-healthcheck-protocol: tcp
      service.beta.kubernetes.io/aws-load-balancer-healthcheck-port: "10084"
```

The example is for `ip` targets. The Service's target port is the named port
`invocation-tcp`, so use the numeric `10084` only with `ip` targets.

Check the target health in your load balancer after the Service is created, and
confirm that both an HTTP and a gRPC invocation succeed.

Read the address after the Service has one:

```bash
kubectl -n nvcf get svc worker-callback-proxy \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}{.status.loadBalancer.ingress[0].ip}{"\n"}'
```

An AWS load balancer returns a hostname; other platforms may return an IP
address.

### Why not a nip.io style name

A wildcard DNS service such as nip.io maps a name to an IP address written in
that name. The callback name already starts with an IP address, the
control-plane pod's, so such a service does not send it to the proxy. For
example, with a worker domain of `203-0-113-10.nip.io`, the callback name
`10-0-0-12.203-0-113-10.nip.io` resolves to `10.0.0.12`, the pod IP, which the
compute cluster cannot reach, and gRPC invocations fail with
`DEADLINE_EXCEEDED`. A domain built from the Gateway address, such as one you
might use for the ingress domain, has the same problem and would point at the
Gateway in any case. Use a DNS name that you control, and map the wildcard to
the proxy address yourself.

### Resolve `*.<workerDomain>` on an EKS compute cluster

Use one of these. Both map the whole wildcard to the proxy address, so
`<dashed-pod-ip>.<workerDomain>` and `invocation.<workerDomain>` both resolve.

**Option A: a Route 53 private hosted zone.** Create a private hosted zone for
`<workerDomain>` and associate it with the VPC of the compute cluster, then add
a wildcard record that points at the load balancer hostname. With
`PROXY_LB` set to the hostname from the previous step:

```bash
cat > worker-callback-dns.json <<EOF
{
  "Changes": [{
    "Action": "UPSERT",
    "ResourceRecordSet": {
      "Name": "*.<workerDomain>",
      "Type": "CNAME",
      "TTL": 60,
      "ResourceRecords": [{"Value": "${PROXY_LB}"}]
    }
  }]
}
EOF
aws route53 change-resource-record-sets \
  --hosted-zone-id <hosted-zone-id> --change-batch file://worker-callback-dns.json
```

If the platform returns an IP address, use an `A` record with that address.

To roll back, run the same command with `"Action": "DELETE"` and the same
record.

**Option B: a CoreDNS rewrite in the compute cluster.** Use this when you cannot
create a zone. Back up the CoreDNS configuration first:

```bash
kubectl -n kube-system get configmap coredns -o yaml > coredns-backup.yaml
```

Edit `kube-system/coredns`, and add this block inside the server block of the
Corefile (`.:53 { ... }`), with the dots of the domain escaped and
`<proxy-host>` set to the load balancer hostname, which the compute cluster must
be able to resolve:

```text
rewrite stop {
  name regex (.+)\.grpc-worker\.example\.com\.$ <proxy-host>.
  answer auto
}
```

Then restart CoreDNS:

```bash
kubectl -n kube-system rollout restart deployment/coredns
kubectl -n kube-system rollout status deployment/coredns
```

To roll back, reapply `coredns-backup.yaml` and restart CoreDNS again. If the
CoreDNS add-on is managed by your platform, an add-on update can revert a manual
edit, so check the rule again after updating the add-on.

### Validate from the compute cluster

Run these in the compute cluster, after the stack is applied, and before you
invoke a function.

1. Resolve both kinds of name from a pod. Use any image that has `nslookup`:

   ```bash
   kubectl run dns-check --rm -it --restart=Never --image=<image-with-nslookup> -- \
     nslookup 10-0-0-1.<workerDomain>
   kubectl run dns-check --rm -it --restart=Never --image=<image-with-nslookup> -- \
     nslookup invocation.<workerDomain>
   ```

   Both must return the load balancer address, not `10.0.0.1`. An answer of the
   pod IP means a name-embedded-IP DNS service is answering.

2. Check the HTTP frontend certificate. From a host or pod that can reach the
   load balancer, with the CA from
   [Give workers the CA](#give-workers-the-ca) when the issuer is the stack's:

   ```bash
   openssl s_client -connect invocation.<workerDomain>:10084 \
     -servername invocation.<workerDomain> \
     -verify_hostname invocation.<workerDomain> \
     -CAfile worker-callback-ca.pem </dev/null
   ```

   `-servername` only sends the name. `-verify_hostname` checks it against the
   certificate's DNS names, which must include `*.<workerDomain>`. The output
   must end with `Verify return code: 0 (ok)`.

3. Invoke a real HTTP function and a real gRPC function several times. The
   gRPC call exercises UDP 443, which a TCP check cannot test. See
   [Verify](#verify) for the proxy's access log.

## Certificates and CAs

A callback uses a TLS session from the worker to the proxy. From the proxy to
the pod it uses a second, separate TLS session, unless you send plain HTTP to
the invocation-service pod (below). The two sessions never share a certificate:

| Session | Client | Server certificate | Client verifies with | Set by |
| --- | --- | --- | --- | --- |
| Worker to proxy (HTTP on TCP 10084, gRPC on UDP 443) | Worker | Proxy frontend certificate for `*.<workerDomain>` | The worker's CA bundle: the stack CA for the default issuer, its system roots for a public certificate | `workerCallbackProxy.tls.frontend` |
| Proxy to invocation-service pod (TCP 8443) | Proxy | Pod certificate for `*.invocation.nvcf.svc.cluster.local`, issued by the stack CA | `ca.crt` of Secret `invocation-service-worker-tls`, and the pod name | `workerCallbackProxy.invocation.upstreamTls.enabled` |
| Proxy to grpc-proxy pod (UDP 10084) | Proxy | Pod certificate for `*.grpc-headless.nvcf.svc.cluster.local`, issued by the stack CA | `ca.crt` of Secret `grpc-proxy-worker-tls`, and the fixed name `*.grpc-headless.nvcf.svc.cluster.local` | Always on |

The pod certificates always come from the stack CA. Only the frontend
certificate is a choice, and workers never see the pod certificates, so workers
never need their CA.

### Pod certificate Secrets

The proxy reads the CA that verifies each pod from the Secret that holds that
pod's certificate:

| Pod | Secret (default) | Key | Where the name is set |
| --- | --- | --- | --- |
| invocation-service | `invocation-service-worker-tls` | `ca.crt` | The stack names the Secret once and passes the same name to the invocation-service certificate and to the proxy (`invocation.upstream.tls.caSecret.name`), so the two cannot differ |
| grpc-proxy | `grpc-proxy-worker-tls` | `ca.crt` | The grpc-proxy chart's default name for its worker certificate. The proxy chart's `grpcProxy.caSecret` defaults to it. |

You do not set these in the stack. They matter when you use the proxy chart on
its own or want to inspect a certificate. Envoy reloads the CA when the Secret
changes, so a rotated CA needs no restart.

### Plain HTTP to the invocation-service pod

To send plain HTTP from the proxy to the invocation-service pod, set
`workerCallbackProxy.invocation.upstreamTls.enabled: false`. The stack then
uses the pod's plaintext port 8080 and does not turn on its worker TLS
listener. The HTTP callback then has one TLS session, from the worker to the
proxy. Use this only where the network between the proxy and the pod is
trusted. gRPC is not affected.

### Callback path and CONNECT

The proxy routes a forward-proxy callback only to the worker callback path,
`/v2/nvcf/worker/request-attach`, because the pod's worker port also serves the
rest of the invocation API. A request for any other path gets a 404.

The stack gives workers `http://` targets, which workers send as forward-proxy
requests, so it turns CONNECT tunnels off. A CONNECT tunnel is a raw TCP
connection to the pod that the proxy cannot read or restrict, and the worker
runs its own TLS session through it, so that worker needs the stack CA. Turn
CONNECT back on only for workers older than worker-utils 1.3.2:

```yaml
workerCallbackProxy:
  invocation:
    connect:
      enabled: true
```

## Use a public certificate on the proxy

By default the proxy frontend certificate comes from the stack's OpenBao
issuer, so workers need the stack CA. To let workers validate it with their
system roots, for example for workers that another scheduler starts and that
cannot receive a per-stack CA, give the frontend its own certificate. Use an
issuer for a public CA, such as a Let's Encrypt `ClusterIssuer` with DNS-01
validation for `*.<workerDomain>`:

```yaml
workerCallbackProxy:
  enabled: true
  workerDomain: grpc-worker.example.com
  tls:
    frontend:
      certificate:
        issuerRef:
          kind: ClusterIssuer
          name: letsencrypt
```

Or point at an existing `kubernetes.io/tls` Secret for `*.<workerDomain>`:

```yaml
workerCallbackProxy:
  tls:
    frontend:
      existingSecret: worker-public-tls
```

The frontend certificate covers both the HTTP and the gRPC callbacks. The proxy
reloads it when the Secret changes, so a renewal needs no restart. The pod
certificates stay on the stack CA, and the proxy verifies them. With a public
frontend certificate you do not need `workerCallbackCaPem`, so skip the next
section.

## Give workers the CA

Skip this section when the frontend certificate comes from a public CA.
Otherwise workers trust only the system roots, so they reject the proxy's
certificate until they receive the issuing CA.

1. Read the CA from the control plane after the proxy certificate is issued:

   ```bash
   kubectl -n nvcf get secret worker-callback-proxy-frontend-tls \
     -o jsonpath='{.data.ca\.crt}' | base64 -d > worker-callback-ca.pem
   ```

2. In the compute-plane environment file, set the PEM:

   ```yaml
   global:
     nvcaOperator:
       agent:
         workerCallbackCaPem: |
           -----BEGIN CERTIFICATE-----
           ...
           -----END CERTIFICATE-----
   ```

3. Apply the compute-plane stack. NVCA passes the CA to function pods as
   `NVCF_PROXY_CA_PEM`. Only function pods created after the change receive
   it, so redeploy existing functions.

## Verify

```bash
kubectl -n nvcf get deploy worker-callback-proxy invocation-service grpc-proxy-deployment
kubectl -n nvcf get certificate
```

Invoke an HTTP function and a gRPC function several times. The proxy's access
log shows each callback and the pod it was forwarded to:

```bash
kubectl -n nvcf logs deploy/worker-callback-proxy
```
