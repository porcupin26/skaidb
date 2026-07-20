# Kerberos (GSSAPI) authentication

skaidb can authenticate clients with a **Kerberos ticket** instead of a
password, following an external-authentication model: a KDC vouches for
the principal, and skaidb only maps that principal to a role. Both the binary
protocol (drivers, `skaidbsh`) and REST/HTTP (browser SSO via SPNEGO) support
it, alongside — not instead of — SCRAM password auth.

## Availability

Kerberos links the system Kerberos C library, so it is compiled into the
**glibc `.deb`/`.rpm`** builds (and macOS/Windows client builds when enabled),
but **not the static-musl `.tar.gz`** — that binary ships without it. Starting
a musl build with `auth.gssapi_enabled = true` is a hard startup error, never a
silent no-op. The `.deb` declares a runtime dependency on `libgssapi-krb5-2`
(`.rpm`: `krb5-libs`), so a minimal host pulls the library automatically.

## Prerequisites

- A Kerberos realm and KDC (MIT krb5, Active Directory, FreeIPA, …).
- A **service principal** for skaidb, conventionally `skaidb/<host>@REALM`,
  and its **keytab** exported to each skaidb node.
- Clients with a ticket-granting ticket (from `kinit`, a login manager, or a
  client keytab).
- Clocks synchronized within the KDC's skew tolerance (default 5 minutes) —
  Kerberos rejects tickets outside it. Run NTP everywhere.

### Create the service principal and keytab (MIT krb5)

On the KDC:

```sh
kadmin.local -q "addprinc -randkey skaidb/node1.example.com@EXAMPLE.COM"
kadmin.local -q "ktadd -k /etc/skaidb/skaidb.keytab skaidb/node1.example.com@EXAMPLE.COM"
```

Copy `skaidb.keytab` to the node, own it by the skaidb user, and lock it down:

```sh
chown skaidb:skaidb /etc/skaidb/skaidb.keytab
chmod 640 /etc/skaidb/skaidb.keytab
```

## Server configuration

In `/etc/skaidb/skaidb.toml`:

```toml
[auth]
scram_enabled = true            # SCRAM stays available alongside GSSAPI
gssapi_enabled = true
gssapi_keytab = "/etc/skaidb/skaidb.keytab"
# Optional: accept only this exact SPN. Empty accepts whatever the keytab holds
# (the usual case — the acceptor tries every key in the keytab).
gssapi_service_principal = "skaidb/node1.example.com@EXAMPLE.COM"

[encryption]
# Strongly recommended: GSSAPI authenticates the client but does NOT encrypt
# the SQL stream. Run it inside client TLS for confidentiality.
client_tls = "required"
```

Environment-variable equivalents (e.g. for Docker): `SKAIDB_GSSAPI_ENABLED`,
`SKAIDB_GSSAPI_KEYTAB`, `SKAIDB_GSSAPI_SERVICE_PRINCIPAL`.

The server reads the keytab once at startup (as the GSSAPI `KRB5_KTNAME`) and
resolves the realm from the ambient `/etc/krb5.conf`. A missing or unreadable
keytab fails startup loudly.

### Create the external users

A Kerberos user is **external** — passwordless, keyed by its principal. The
principal contains `@` and `.`, so double-quote it:

```sql
CREATE USER "alice@EXAMPLE.COM" GSSAPI;
GRANT SELECT ON DATABASE app TO "alice@EXAMPLE.COM";
```

The principal maps **exactly** to its own-named role; grants and role
inheritance work identically to password users. An external user cannot
authenticate with a password, and a password user is never reachable through
the Kerberos path.

## Client usage

### `skaidbsh`

```sh
kinit alice@EXAMPLE.COM
skaidbsh -H node1.example.com:7000 --tls --tls-ca /etc/skaidb/tls/ca.crt \
         --auth-mechanism gssapi \
         --gssapi-spn skaidb/node1.example.com@EXAMPLE.COM \
         -u alice@EXAMPLE.COM \
         -e "SELECT 1"
```

No password is sent — the ambient ticket cache is used. `--user` is the client
principal; the authenticated identity comes from the ticket. Environment
equivalents: `SKAIDB_AUTH_MECHANISM=gssapi`, `SKAIDB_GSSAPI_SPN=…`.

### Rust driver

```rust
// kinit first; uses the ambient ticket cache.
let client = Client::connect_gssapi_tls(
    &["node1.example.com:7000".into()],
    "alice@EXAMPLE.COM",                       // client principal
    "skaidb/node1.example.com@EXAMPLE.COM",    // target service principal
    Some(tls),                                 // recommended
)?;
```

### REST / browser (SPNEGO)

When `gssapi_enabled`, the REST endpoints accept `Authorization: Negotiate
<token>` (RFC 4559) and advertise it in the `401 WWW-Authenticate` challenge
(alongside Basic). A Kerberos-configured browser gets single-sign-on to the
UI/REST; `curl --negotiate -u : https://node1.example.com:7443/query -d …`
works too.

> **SPN note for HTTP clients:** browsers and `curl --negotiate` request the
> service principal `HTTP/<host>@REALM` (the HTTP convention). To serve them,
> add an `HTTP/<host>@REALM` principal to the keytab as well — the acceptor
> tries every key in the keytab. Native skaidb clients target `skaidb/<host>`
> directly, so they need only that one.

SPNEGO over REST is single-leg (Kerberos establishes in one token); the
stateless REST path does not carry a multi-round negotiation.

## How it works

- **Mechanism negotiation.** The client handshake carries a mechanism selector
  (`AuthStart`); SCRAM is the default and stays wire-compatible, so old clients
  are unaffected. GSSAPI adds a repeatable `AuthToken` frame for the context
  exchange.
- **Context establishment.** The client and server run a GSS context
  negotiation (mutual authentication + confidentiality required); the server
  reads the cryptographically-authenticated principal.
- **Identity mapping.** The principal is looked up as an external user; its
  own-named role is the acting role. The username a client puts in `AuthStart`
  is untrusted — only the GSS principal counts.

## Operational caveats

- **Clock skew.** The most common failure. Symptom: `Clock skew too great`.
  Fix: NTP on the KDC, nodes, and clients.
- **DNS / SPN mismatch.** The client's target SPN must match a key in the
  server's keytab and a principal the KDC knows. Reverse-DNS canonicalization
  can rewrite the host in the SPN unexpectedly; disable it (`rdns = false` in
  `krb5.conf`) or pin `gssapi_service_principal` and pass the client a matching
  `--gssapi-spn`.
- **Keytab permissions.** The keytab is a long-term secret. Keep it
  `0640 skaidb:skaidb`, never world-readable, never in version control.
- **TLS.** GSSAPI authenticates; it does not encrypt the SQL stream. Run
  `client_tls = required` for confidentiality (see the encryption docs).
- **Platform.** glibc only today. The static-musl build has no Kerberos;
  macOS/Windows client builds are added as their CI builds are validated.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `gssapi init failed (did you kinit?)` on the client | No ticket — run `kinit`, check `klist`. |
| `no GSSAPI user for principal "…"` | The principal has no `CREATE USER "…" GSSAPI` account. |
| `GSSAPI authentication is not enabled` | `auth.gssapi_enabled` is false, or a non-kerberos (musl) build. |
| Startup: `gssapi_enabled = true but this build lacks the kerberos feature` | Running the musl binary — use the glibc `.deb`/`.rpm`. |
| `Clock skew too great` | Time drift — sync NTP. |
| `Server not found in Kerberos database` | The target SPN isn't a KDC principal / not in the keytab. |

## Security scope (current)

The GSS context requires mutual authentication and confidentiality, and runs
inside client TLS. Channel binding (binding the GSS context to the outer TLS
channel, `tls-server-end-point`) is a planned hardening — not yet implemented;
until then, always run GSSAPI inside TLS, which already authenticates the
server and prevents token relay to a different endpoint.
