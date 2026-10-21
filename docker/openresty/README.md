# OpenResty unified browser authentication

These files are sanitized deployment examples. Real domains, addresses,
certificates, passwords, hashes, session secrets, and API credentials remain
private deployment state.

## Boundary

`auth.example.com` is the only normal browser credential entry. Its same-origin
login page sends an `Authorization` header to a POST-only Nginx `auth_basic`
endpoint. Successful Nginx authentication returns a 30-second, service-bound
handoff ticket. The page POSTs that ticket to the selected service, which consumes
it once and issues an eight-hour signed, host-only `sudox_platform_session`
cookie. No parent-domain bearer cookie is created, so unrelated sibling hosts
cannot receive or replay a browser session.

The UI services are:

- `quant`, `vibe`, `stock`, `nautilus`, `learn`, `ragflow`, and `raggraph`.
- Service keys and destinations are fixed in `sudox-platform-auth.lua`; callers
  cannot provide a return URL.
- The login page cross-checks both the returned destination and consume endpoint,
  then uses a form POST so the handoff ticket never appears in a URL.
- Browser cookies are service-bound as well as host-only. A session issued by one
  UI cannot be replayed against another UI.

Machine/API contracts deliberately remain separate:

- Quant `/api/` keeps its existing Basic Auth behavior.
- RAGFlow `/api/` keeps application/API authentication and bypasses the browser
  session.
- RagGraph `/api/` loads a private `raggraph-api-compat.conf`. Inventory its
  callers before replacing the historical static gateway credential with a
  rotatable signed token or application-authenticated path.

## Files

- `16.conf`: authentication virtual host.
- `12.conf` through `15.conf`: Quant UI services plus Quant API compatibility.
- `17.conf` through `19.conf`: Learn, RAGFlow, and RagGraph examples.
- `sudox-platform-auth.lua`: fixed service registry, one-time handoff, host-only
  session issuance, validation, rotation overlap, redirect, and logout.
- `sudox-platform-browser-session.inc`: common per-service consume/logout
  locations; include it only inside each protected UI `server {}` after setting
  `$sudox_platform_service`.
- `sudox-platform-http.conf`: login rate-limit and handoff replay zones; include
  it once in `http {}`.
- `platform-login/`: no-build login page with external CSS and JavaScript so a
  strict Content Security Policy can be used.
- `quant-platform-auth.lua`: unchanged legacy implementation retained while any
  private pre-migration configuration still uses its old function contract.

## Private material

Create the current signing secret on the gateway host. It must contain at least
32 random bytes and must never be committed, printed, or copied into evidence:

```bash
umask 077
openssl rand -hex 32 > /opt/om/nginx/conf/sudox-platform-session-secret
# Use 0600 only when workers run as the owning user. Otherwise set the verified
# OpenResty worker group and use 0640; never make the secret world-readable.
openresty_worker_group="replace-with-verified-worker-group"
chown "root:${openresty_worker_group}" /opt/om/nginx/conf/sudox-platform-session-secret
chmod 640 /opt/om/nginx/conf/sudox-platform-session-secret
```

Use one private `/opt/om/nginx/conf/sudox-platform.htpasswd` for the browser
entry. Do not copy hashes into Git. The deployed Quant header include and
RagGraph API compatibility include are also secret-bearing private files.

For a controlled signing-key rotation, move the old current key to
`sudox-platform-session-secret.previous`, install a new current key, and reload
OpenResty. Existing per-service sessions remain valid for at most one TTL. Remove the
previous file after eight hours and reload again. An emergency revocation omits
the previous key, which intentionally signs every user out.

## Verification before production

1. Replace only placeholders in a private staging copy.
2. Include `sudox-platform-http.conf` once from `http {}`.
3. Install `sudox-platform-auth.lua`, `sudox-platform-browser-session.inc`,
   `platform-login/`, the current secret, unified htpasswd, and required private
   compatibility includes. Keep the existing `quant-platform-auth.lua` in place
   until an inventory proves no pre-migration consumer remains.
4. Run `nginx -t` inside the same OpenResty image/module set as production.
5. Exercise valid/invalid login, all seven service keys, an unknown service,
   expired/replayed handoffs, cross-service cookie rejection, tampered/expired
   cookies, local logout, direct UI entry, and existing API clients.
6. Confirm access/error logs contain no Authorization value, password, cookie,
   htpasswd hash, or secret.

The repository's isolated syntax and HTTP matrix uses a pinned copy of the
production OpenResty Manager image and only generated dummy credentials:

```bash
bash scripts/verify_openresty_gateway.sh
```

## Staged production cutover (separate Human Gate)

Production work is not authorized by the repository examples alone.

1. Back up the exact active `sites/`, relevant `upstreams/`, private includes,
   htpasswd files, and session modules. Record restore paths without secret
   contents.
2. Install the auth virtual host inactive and verify it with host/SNI-directed
   requests before normal traffic reaches it.
3. Deploy the new module and shared include without replacing the legacy module;
   mixed old/new configurations must pass before the first UI cutover.
4. Canary Learn, then Quant UI services, then RAGFlow UI. Cut over RagGraph UI
   last, only after its API callers pass the compatibility matrix.
5. At every stage test login, direct entry, reload, logout, expiry/tampering,
   API clients, upstream health, and logs.
6. Only after every old caller is removed may the legacy module be archived;
   replacing it is not part of a mixed-version rollout.
7. Rotate and revoke the historical RagGraph static gateway credential during
   its separately approved stage.

Rollback restores the backed-up site and upstream files, reloads the verified
OpenResty master process, and retests both UI and API routes. Some management
images retain a stale PID file, so verify the live master PID rather than
assuming `nginx -s reload` succeeded.
