# ICMS API keys

Self-managed ICMS (`sis`) accepts `Bearer nvapi-*` keys issued with an ICMS
policy. API Keys evaluates the key under namespace `icms` with rule
`apikey.allow` and the `apiKey` request field. The ICMS audience is
`icms-api`. A key issued only for Cloud Functions or Cloud Tasks cannot
authorize ICMS calls.

The stack registers the audience in API Keys and gives the ICMS service account
an OpenBao-signed service token for outbound evaluation. Deploy matching API Keys,
OpenBao migrations, ICMS image, and ICMS chart releases before enabling the
ICMS API-key feature flag. The stack orders API Keys before ICMS.

## Issue an ICMS key

An operator with permission to create API keys can call the API Keys service.
Authenticate as for other self-managed API Keys operations and supply the
same issuer and owner headers used for the key's issuing service. For a key
issued by ICMS, set `Key-Issuer-Id` to `icms-api` and include that audience in
`audience_service_ids`. For a Cloud Functions-issued key that also calls ICMS,
use the Cloud Functions issuer ID and include both audiences and both policies.
The Cloud Functions issuing service registration permits the ICMS audience.

For example, a policy granting cluster listing scopes is:

```json
{
  "policies": [
    {
      "aud": "icms-api",
      "scopes": ["cluster_listing", "read:cluster-management"],
      "resources": [{"type": "account-clusters", "id": "*"}]
    }
  ]
}
```

Set `description`, `expires_at`, and `authorizations` in the API Keys
`POST /v1/keys` request, and set `audience_service_ids` to the ICMS audience.
Use the resulting key as `Authorization: Bearer <key>` when calling ICMS.

ICMS caches successful key evaluations for one minute. If API Keys is
unavailable, it can reuse a previously validated result for the same key for
up to 60 minutes. Rejected keys are not cached. Revocations can therefore take
up to one minute to take effect during normal operation and up to 60 minutes
during an API Keys outage.

## Permissions

ICMS maps policy scopes to `apikey:<scope>` authorities. Controllers check
the matching authority. Existing service-level validation also applies to
API-key callers. ICMS does not impose new API-key-only account or resource
checks on these routes.

| ICMS operation | Scopes | Existing identity behavior |
| --- | --- | --- |
| Account GPU, region, cluster name, attribute, and instance-type queries | Matching `gpu_listing`, `regions_listing`, `clusters_listing`, `attributes_listing`, or `instance_types` | Uses the path account |
| Account availability and GPU usage | `gpu-usage` or `spot-gpu-usage` | Uses the path account and optional deployment |
| Cluster-group listing | `cluster_listing` | Uses the path account |
| Create a cluster | `cluster-management` | Uses the path account |
| List clusters | `read:cluster-management` or `cluster-management` | Uses the path account |
| Read a cluster | `read:cluster-management` or `cluster-management` | Uses the path account and cluster |
| Update or delete a cluster | `cluster-management` | Uses the path account and cluster |
| Create an instance request | `instance-request` | Customer is the key's `ownerId` |
| Read or terminate instances under a workload | `instance-request` | Uses the path account and workload |
| Terminate one instance by ID | `instance-request` | Uses the path account and instance |
| Describe, cancel, or terminate by action on `/v1/si` | `instance-request` | Customer is the key's `ownerId`; only requests or instances stored for that customer are selected |
| Describe requests across customers on `/v1/si/admin` | `admin:spot-request:describe` or `admin:instance-request:describe` | No customer filter; issue only to administrators |
| Create or delete tenant registrations | `tenant_registration` | Uses the path account; delete verifies the stored account and tenant |
| NVCA cluster registration, heartbeat, credentials, and JWKS endpoints | `nvca-cluster` | Uses the cluster identity from the key's `cluster` resource |

Self-managed callers of the action-based routes include Cloud Functions and
Cloud Tasks. Their JWT subjects are `nvcf-api` and `nvct-api`, respectively.
API-key requests use the evaluated key's `ownerId` as that same customer
identifier. Existing `apikey:nvca-cluster` and `apikey:billing_override`
admission on internal status updates and billing overrides remains available.
JWT and cluster-identity authentication continue to use their existing scopes.
