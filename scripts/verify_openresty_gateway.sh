#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
openresty_image=${OPENRESTY_TEST_IMAGE:-uusec/openresty-manager@sha256:be49bec115b30c69fb19dbd7bb84f3a72499d8bb73abedd85f60d78111a2375b}
fixture_root=$(mktemp -d /tmp/quantstack-openresty-auth.XXXXXX)
private_conf="${fixture_root}/private"
container_name="quantstack-openresty-auth-$$"

cleanup() {
  docker stop "${container_name}" >/dev/null 2>&1 || true
  find "${fixture_root}" -type f -delete 2>/dev/null || true
  find "${fixture_root}" -type d -depth -delete 2>/dev/null || true
}
trap cleanup EXIT

for command_name in curl docker htpasswd openssl rg; do
  command -v "${command_name}" >/dev/null
done

mkdir -p \
  "${fixture_root}/certs" \
  "${fixture_root}/logs" \
  "${fixture_root}/client_body_temp" \
  "${fixture_root}/proxy_temp" \
  "${fixture_root}/fastcgi_temp" \
  "${fixture_root}/uwsgi_temp" \
  "${fixture_root}/scgi_temp" \
  "${private_conf}/platform-login"

cp "${repo_root}/tests/fixtures/openresty/nginx.conf" "${fixture_root}/nginx.conf"
cp "${repo_root}/docker/openresty/sudox-platform-auth.lua" "${private_conf}/sudox-platform-auth.lua"
cp "${repo_root}/docker/openresty/sudox-platform-browser-session.inc" "${fixture_root}/sudox-platform-browser-session.inc"
cp "${repo_root}/docker/openresty/platform-login/"* "${private_conf}/platform-login/"

touch \
  "${fixture_root}/ssl.conf" \
  "${fixture_root}/log.conf" \
  "${fixture_root}/acme_challenge.conf" \
  "${fixture_root}/force_ssl.conf" \
  "${fixture_root}/proxy_ip.conf" \
  "${fixture_root}/proxy_custom.conf" \
  "${fixture_root}/websocket.conf" \
  "${fixture_root}/sse.conf" \
  "${fixture_root}/error_page.conf" \
  "${fixture_root}/quant-platform-headers.conf" \
  "${fixture_root}/raggraph-api-compat.conf"

openssl req -x509 -newkey rsa:2048 -nodes -subj /CN=example.com \
  -keyout "${fixture_root}/certs/2.key" \
  -out "${fixture_root}/certs/2.crt" -days 1 >/dev/null 2>&1
openssl rand -hex -out "${private_conf}/sudox-platform-session-secret" 32
htpasswd -bc "${private_conf}/sudox-platform.htpasswd" gateway-test-user gateway-test-password \
  >/dev/null 2>&1
chmod -R a+rX "${fixture_root}"

docker run --rm --entrypoint openresty \
  -v "${fixture_root}:/tmp/gateway" \
  -v "${repo_root}/docker/openresty:/test:ro" \
  "${openresty_image}" -p /tmp/gateway -c nginx.conf -t

docker run -d --rm --name "${container_name}" -p 127.0.0.1::443 \
  -v "${fixture_root}:/tmp/gateway" \
  -v "${repo_root}/docker/openresty:/test:ro" \
  -v "${private_conf}:/opt/om/nginx/conf:ro" \
  --entrypoint openresty "${openresty_image}" \
  -p /tmp/gateway -c nginx.conf -g "daemon off;" >/dev/null

# This base image creates /opt/om/nginx without execute bits because that path is
# normally supplied by its manager volume. The production manager volume already
# owns the path; the isolated fixture must make the synthetic parent traversable.
docker exec "${container_name}" chmod 755 /opt/om/nginx
gateway_port=$(docker port "${container_name}" 443/tcp | sed -E 's/.*:([0-9]+)$/\1/' | head -1)

for _ in {1..20}; do
  if [[ $(curl -sk --resolve "auth.example.com:${gateway_port}:127.0.0.1" \
    -o /dev/null -w '%{http_code}' "https://auth.example.com:${gateway_port}/login/stock") == 200 ]]; then
    break
  fi
  sleep 1
done

login_status=$(curl -sk --resolve "auth.example.com:${gateway_port}:127.0.0.1" \
  -o "${fixture_root}/login.html" -w '%{http_code}' \
  "https://auth.example.com:${gateway_port}/login/stock")
invalid_status=$(curl -sk --resolve "auth.example.com:${gateway_port}:127.0.0.1" \
  -D "${fixture_root}/invalid.headers" -o /dev/null -w '%{http_code}' \
  -X POST -u gateway-test-user:wrong \
  "https://auth.example.com:${gateway_port}/session/stock")
valid_status=$(curl -sk --resolve "auth.example.com:${gateway_port}:127.0.0.1" \
  -c "${fixture_root}/gateway.cookies" -o "${fixture_root}/session.json" -w '%{http_code}' \
  -X POST -u gateway-test-user:gateway-test-password \
  "https://auth.example.com:${gateway_port}/session/stock")
resume_status=$(curl -sk --resolve "auth.example.com:${gateway_port}:127.0.0.1" \
  -b "${fixture_root}/gateway.cookies" -o "${fixture_root}/resume.json" -w '%{http_code}' \
  -X POST "https://auth.example.com:${gateway_port}/handoff/quant")
handoff_ticket=$(sed -n 's/.*"ticket":"\([^"]*\)".*/\1/p' "${fixture_root}/session.json")
consume_status=$(curl -sk --resolve "stock-screener.example.com:${gateway_port}:127.0.0.1" \
  -c "${fixture_root}/cookies" -D "${fixture_root}/consume.headers" -o /dev/null -w '%{http_code}' \
  --data-urlencode "ticket=${handoff_ticket}" \
  "https://stock-screener.example.com:${gateway_port}/platform-auth/consume")
replay_status=$(curl -sk --resolve "stock-screener.example.com:${gateway_port}:127.0.0.1" \
  -o /dev/null -w '%{http_code}' --data-urlencode "ticket=${handoff_ticket}" \
  "https://stock-screener.example.com:${gateway_port}/platform-auth/consume")
redirect_status=$(curl -sk --resolve "stock-screener.example.com:${gateway_port}:127.0.0.1" \
  -D "${fixture_root}/redirect.headers" -o /dev/null -w '%{http_code}' \
  "https://stock-screener.example.com:${gateway_port}/")
logout_status=$(curl -sk --resolve "stock-screener.example.com:${gateway_port}:127.0.0.1" \
  -b "${fixture_root}/cookies" -o /dev/null -w '%{http_code}' -X POST \
  "https://stock-screener.example.com:${gateway_port}/platform-auth/logout")
gateway_logout_status=$(curl -sk --resolve "auth.example.com:${gateway_port}:127.0.0.1" \
  -b "${fixture_root}/gateway.cookies" -o /dev/null -w '%{http_code}' -X POST \
  "https://auth.example.com:${gateway_port}/logout")

[[ "${login_status}" == 200 ]]
[[ "${invalid_status}" == 401 ]]
[[ "${valid_status}" == 200 ]]
[[ "${resume_status}" == 200 ]]
[[ -n "${handoff_ticket}" ]]
[[ "${consume_status}" == 303 ]]
[[ "${replay_status}" == 401 ]]
[[ "${redirect_status}" == 302 ]]
[[ "${logout_status}" == 204 ]]
[[ "${gateway_logout_status}" == 204 ]]
rg -q '统一登录' "${fixture_root}/login.html"
rg -q 'https://stock-screener.example.com:11443/' "${fixture_root}/session.json"
rg -q 'https://quant.example.com:11443/' "${fixture_root}/resume.json"
! rg -qi '^WWW-Authenticate:' "${fixture_root}/invalid.headers"
! rg -qi '^Set-Cookie:.*Domain=' "${fixture_root}/consume.headers"
rg -qi '^location: https://stock-screener.example.com:11443/' "${fixture_root}/consume.headers"
rg -qi '^location: https://auth.example.com:11443/login/stock' "${fixture_root}/redirect.headers"

printf 'OpenResty gateway: PASS (login=%s invalid=%s valid=%s resume=%s consume=%s replay=%s redirect=%s logout=%s gateway_logout=%s)\n' \
  "${login_status}" "${invalid_status}" "${valid_status}" "${resume_status}" \
  "${consume_status}" "${replay_status}" "${redirect_status}" "${logout_status}" \
  "${gateway_logout_status}"
