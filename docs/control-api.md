# Realm control API

HTTP/1.1 + JSON over a unix domain socket. Everything here is the contract an
agent programs against; it is versioned by `schema_version`, reported by
`/v1/version`.

```sh
realm -c /etc/realm/realm.toml --control-socket /run/realm/realm.sock
curl --unix-socket /run/realm/realm.sock http://localhost/v1/status
```

The socket is created with mode 0700, and so is a directory realm has to create
for it. An existing directory keeps its permissions — realm warns instead of
tightening a shared one behind you. There is no TCP listener: reachability is a
filesystem question.

## Model

Realm serves a **desired state**: the complete set of endpoints a node should
have, published under a **generation** the caller owns.

- **Generations are yours.** Any monotonically increasing integer. Realm never
  invents one.
- **Resubmitting a generation is free.** The same generation replays the first
  answer — no duplicate endpoint, no second disturbance to traffic. Retry
  after a timeout without thinking about it.
- **An older generation is refused** with `409` and the active generation in
  the body.
- **Endpoints succeed or fail one at a time.** A generation where something
  failed is `partially-applied`; it still becomes the active generation, and
  the failure is healed by submitting a *later* generation, not by retrying the
  same one.
- **Only what changed is touched.** An endpoint whose description is unchanged
  and which is running is reported `unchanged` and is not disturbed in any way.

The smallest unit that can succeed or fail is `(id, protocol)` — a rule serving
both tcp and udp has two of them, and they are independent.

## Endpoints

### `PUT /v1/desired-state`

`POST` is accepted as well. Body:

```json
{
  "generation": 42,
  "endpoints": [
    {
      "id": "rule-8f3a",
      "listen": "0.0.0.0:5000",
      "remote": "10.0.0.7:443",
      "extra_remotes": ["10.0.0.8:443"],
      "balance": "roundrobin: 3, 1",
      "through": "10.0.0.1",
      "network": { "use_udp": true },
      "update_drain_timeout": null,
      "delete_drain_timeout": 30
    }
  ]
}
```

Every field except `id` is an ordinary realm endpoint configuration — the same
shape the TOML file uses, so a renderer can emit one and reuse it for the other.

`id` is **yours and opaque to realm**: it is the key the diff is computed
against. Keep it stable across generations for the same rule, or realm will see
a delete plus a create.

An empty `endpoints` array is legal and means *remove everything*. It is
applied like any other generation, with a warning in the audit log.

Answer, `200`:

```json
{
  "generation": 42,
  "state": "applied",
  "results": [
    { "id": "rule-8f3a", "protocol": "tcp", "action": "updated" },
    { "id": "rule-8f3a", "protocol": "udp", "action": "failed",
      "error": "failed to bind 0.0.0.0:5000: Address already in use (os error 98)",
      "retryable": true }
  ]
}
```

- `state` — `applied` or `partially-applied`
- `action` — `unchanged`, `created`, `updated`, `draining`, `deleted`, `failed`
- `error` — present only on `failed`
- `retryable` — present only on `failed`; see [error classification](#error-classification)

### `GET /v1/status`

```json
{
  "active_generation": 42,
  "generation_state": "applied",
  "ready": true,
  "endpoints": [
    {
      "id": "rule-8f3a",
      "slots": [
        {
          "protocol": "tcp",
          "state": "running",
          "generation": 42,
          "listen": "0.0.0.0:5000",
          "connections": 137,
          "draining": [
            { "generation": 41, "connections": 4, "age_secs": 812.4, "draining_for_secs": 63.1 }
          ]
        }
      ]
    }
  ],
  "process": {
    "version": "2.10.0",
    "features": "[brutal][batched-udp][proxy][balance][transport][control][multi-thread]",
    "dns": { "nameservers": ["10.0.0.53"] },
    "log_level": "info",
    "log_output": "stdout",
    "nofile_soft": 524288,
    "nofile_hard": 524288
  }
}
```

- `state` — `running`, `draining`, `stopped`, or `failed`. A slot reports
  `running` only after its socket is bound; a serving task that exits on its own
  turns the slot `failed` with an `error`.
- `connections` — live connections (tcp) or associations (udp) on the current
  generation.
- `draining` — one entry per superseded generation that still has traffic.
  **This is how you confirm a drain finished:** the cohort disappears from the
  list once its last connection ends.
- `process` — settings frozen at startup. They are not part of the desired
  state and changing them needs a restart; they are reported so you can detect
  drift from what you believe the node was started with.

### `GET /v1/readiness`

`200` with `{"ready": true, "active_generation": 42, "partial": false}` once
realm is serving. `503` while a snapshot restore is still in progress —
submissions during that window are refused as retryable.

### `GET /v1/version`, `GET /v1/capabilities`

The same document either way:

```json
{
  "implementation": "realm-hot-reload-fork",
  "version": "2.10.0",
  "schema_version": 1,
  "capabilities": ["desired-state-reconcile", "..."],
  "features": "[brutal][...]"
}
```

Probe this before anything else: upstream realm has no control socket at all,
so a failed connect means "not this fork". Use `capabilities` rather than
version comparisons to decide what you may rely on.

#### `client-ca-verify`

A node advertising this token verifies TLS peers against a trust anchor you
supply, and converges on replaced certificate material.

`remote_transport` accepts `ca=<path>`, a PEM file holding one or more trust
anchors:

```
tls;sni=realm.example.internal;ca=/etc/realm/pki/root.pem
```

The anchors in that file **replace** the compiled-in public roots rather than
adding to them — otherwise any publicly trusted CA could still vouch for your
internal name. Every certificate in the file must parse, or the endpoint fails
to build; a bundle that silently lost an entry would leave a node trusting one
anchor during a dual-root rotation with nothing to say so. `ca` together with
`insecure` is rejected, as is an empty `ca=`.

An endpoint whose CA file is missing or unparseable **fails, alone**. It does
not fall back to an unverified connection, it does not disturb its siblings,
and the generation lands as `partially-applied` with that endpoint `failed` and
an error naming the file. Heal it by submitting a later generation, as with any
other per-endpoint failure.

Certificate material is part of an endpoint's desired state. Replace the bytes
behind `ca=`, or behind a listen-side `cert=` / `key=`, and the next generation
you submit rebuilds **only** the endpoints naming that material — even when the
configuration you send is byte-identical to the one before. Endpoints that name
none report `unchanged` and keep their established connections, and the process
is never restarted.

Two consequences worth planning for:

- Realm does not watch or poll these files. Your submission is the only trigger,
  so a rotation converges when you next submit, not when the file changes.
- Resubmitting the *active* generation after material changed is refused as a
  conflict rather than replayed. Replaying the old answer would report
  convergence for material the endpoint is no longer running. Advance the
  generation instead.

**Probe for the token, do not compare versions.** A binary without this feature
does not reject `ca=` — unknown transport options are ignored — so it accepts
the same string and quietly verifies against the public roots instead.

Rotating material safely also depends on your side: write the replacement to a
temporary file and rename it into place. Realm digests the file and then reads
it again at construction, so a torn read costs at most one spurious rebuild —
but only if the file is never observed half-written.

## What a change does to live traffic

| operation | new connections | established connections |
|---|---|---|
| endpoint unchanged | untouched | untouched |
| endpoint updated | use the new configuration once it is bound | **keep running on the old one**, indefinitely by default |
| listen address changed | the new address is bound first | drain on the old address |
| endpoint deleted | refused immediately, the port is free at once | force-closed after 30 s by default |
| udp endpoint changed | new association against the new configuration | **terminated**; the client rebuilds them |

**An update never terminates established tcp connections.** That is deliberate:
a configuration change should not drop traffic. If you need to stop traffic
*now* — a ban, a suspension — express it as a **delete**, or as an update
carrying `update_drain_timeout`. Waiting for an update to cut connections will
wait forever.

Same-address replacement is stop-accept → wait for the socket to be released →
bind. That leaves a short window in which *new* connections are refused;
measured at roughly 3 ms on loopback (see `docs/benchmarks/`). Established
connections never notice it.

### Drain deadlines

| field | unit | absent means | applies to |
|---|---|---|---|
| `update_drain_timeout` | seconds | never force-close | connections superseded by an update |
| `delete_drain_timeout` | seconds | 30 s | connections of a deleted endpoint |

`0` means force-close immediately. Both are per endpoint.

## Error classification

Every error says whether resubmitting the same thing can help.

| situation | http | `kind` | `retryable` |
|---|---|---|---|
| generation older than the active one | 409 | `stale-generation` | no |
| snapshot restore not finished | 503 | `not-ready` | yes |
| body is not valid json | 400 | `malformed-request` | no |
| body over 8 MiB | 413 | `request-too-large` | no |
| unknown path | 404 | `unknown-route` | no |
| internal failure | 500 | `internal` | yes |

Whole-request errors carry:

```json
{ "error": { "kind": "stale-generation", "retryable": false,
             "message": "stale generation, active generation is 42",
             "active_generation": 42 } }
```

Per-endpoint failures inside a `200` carry `retryable` on the result itself:
a bind that lost a race is retryable, an endpoint realm cannot parse is not.

## Taking over from the static configuration

A node started with a TOML file serves it as **generation 0**, under ids
derived from the listen address and the protocols:

```
<protocols>:<listen address>
```

where `<protocols>` is `tcp`, `udp` or `tcpudp`. For example
`tcp:0.0.0.0:5000`, or `tcpudp:[::1]:5000` — the address is formatted exactly
as Rust's `SocketAddr` prints it, so ipv6 keeps its brackets.

Computing the same ids for the equivalent desired state makes your first
submission report `unchanged` for every endpoint: the takeover moves no
traffic. Any generation ≥ 1 works for it; generation 0 is the static one.

## Crash recovery

With `--control-socket`, realm keeps a last-known-good state file next to the
socket (or wherever `--state-file` points). After a restart it restores that
state and serves it immediately, without waiting for your next reconcile, and
reports the generation it came from. The file is written atomically and is
readable by its owner only.

While the restore runs, `/v1/readiness` is `503` and submissions are refused as
retryable. The snapshot is realm's own runtime record — your backend stays the
only source of truth, and your next reconcile converges the node to it.

If the state file cannot be read, realm falls back to the static configuration
and keeps forwarding rather than starting empty.

## What is out of scope for a reconcile

Process-wide and frozen at startup: dns, logging, file-descriptor limits, pipe
capacity, the tls provider, the pre-connect hook, and the relay buffer
parameters. Changing them needs a restart — which drops every connection on the
node, the one remaining full-outage path. `/v1/status` reports the values in
effect so you can detect drift without restarting to find out.
