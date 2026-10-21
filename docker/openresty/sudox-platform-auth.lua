local M = {}

local CURRENT_SECRET_PATH = "/opt/om/nginx/conf/sudox-platform-session-secret"
local PREVIOUS_SECRET_PATH = "/opt/om/nginx/conf/sudox-platform-session-secret.previous"
local COOKIE_NAME = "sudox_platform_session"
local GATEWAY_COOKIE_NAME = "sudox_platform_gateway"
local AUTH_ORIGIN = "https://auth.example.com:11443"
local SESSION_TTL = 8 * 60 * 60
local HANDOFF_TTL = 30
local CLOCK_SKEW = 60
local TOKEN_VERSION = "v3"

local SERVICE_TARGETS = {
    quant = "https://quant.example.com:11443/",
    vibe = "https://vibe-trading.example.com:11443/agent",
    stock = "https://stock-screener.example.com:11443/",
    nautilus = "https://nautilus.example.com:11443/",
    learn = "https://learn.example.com:11443/",
    ragflow = "https://ragflow.example.com:11443/",
    raggraph = "https://raggraph.example.com:11443/",
}

local cached_current_secret
local cached_previous_secret
local secrets_loaded = false

local function read_secret(path, required)
    local file = io.open(path, "rb")
    if not file then
        if required then ngx.log(ngx.ERR, "platform session secret is unavailable") end
        return nil
    end
    local secret = file:read("*a")
    file:close()
    secret = (secret or ""):gsub("%s+$", "")
    if #secret < 32 then
        if required then ngx.log(ngx.ERR, "platform session secret is invalid") end
        return nil
    end
    return secret
end

local function load_secrets()
    if not secrets_loaded then
        cached_current_secret = read_secret(CURRENT_SECRET_PATH, true)
        cached_previous_secret = read_secret(PREVIOUS_SECRET_PATH, false)
        secrets_loaded = true
    end
    return cached_current_secret, cached_previous_secret
end

local function base64url(value)
    return ngx.encode_base64(value):gsub("+", "-"):gsub("/", "_"):gsub("=+$", "")
end

local function secure_equal(left, right)
    if type(left) ~= "string" or type(right) ~= "string" or #left ~= #right then return false end
    local bit = require("bit")
    local difference = 0
    for index = 1, #left do
        difference = bit.bor(difference, bit.bxor(left:byte(index), right:byte(index)))
    end
    return difference == 0
end

local function unsigned_token(audience, service, issued_at, expires_at, nonce)
    local parts = { TOKEN_VERSION, audience, service, issued_at, expires_at }
    if nonce then table.insert(parts, nonce) end
    return table.concat(parts, ".")
end

local function signature(secret, unsigned)
    return base64url(ngx.hmac_sha1(secret, unsigned))
end

local function validate_token(token, audience, service, ttl, has_nonce)
    if not token then return false end
    local version, token_audience, token_service, issued_at, expires_at, nonce, supplied
    if has_nonce then
        version, token_audience, token_service, issued_at, expires_at, nonce, supplied =
            token:match("^([^.]+)%.([^.]+)%.([^.]+)%.(%d+)%.(%d+)%.([A-Za-z0-9_-]+)%.([A-Za-z0-9_-]+)$")
    else
        version, token_audience, token_service, issued_at, expires_at, supplied =
            token:match("^([^.]+)%.([^.]+)%.([^.]+)%.(%d+)%.(%d+)%.([A-Za-z0-9_-]+)$")
    end
    if version ~= TOKEN_VERSION or token_audience ~= audience or token_service ~= service then return false end
    local issued = tonumber(issued_at)
    local expiry = tonumber(expires_at)
    local now = ngx.time()
    if not issued or not expiry or issued > now + CLOCK_SKEW or expiry <= now then return false end
    if expiry - issued ~= ttl or expiry > now + ttl + CLOCK_SKEW then return false end
    local current, previous = load_secrets()
    local unsigned = unsigned_token(audience, service, issued_at, expires_at, nonce)
    local valid = (current and secure_equal(supplied, signature(current, unsigned)))
        or (previous and secure_equal(supplied, signature(previous, unsigned)))
    return valid == true, supplied
end

local function set_no_store_headers()
    ngx.header["Cache-Control"] = "no-store"
    ngx.header["Pragma"] = "no-cache"
    ngx.header["Referrer-Policy"] = "no-referrer"
    ngx.header["X-Content-Type-Options"] = "nosniff"
end

local function issue_cookie(cookie_name, audience, service)
    local current = load_secrets()
    if not current then return false end
    local now = ngx.time()
    local issued_at = tostring(now)
    local expires_at = tostring(now + SESSION_TTL)
    local unsigned = unsigned_token(audience, service, issued_at, expires_at)
    local token = unsigned .. "." .. signature(current, unsigned)
    ngx.header["Set-Cookie"] = cookie_name .. "=" .. token
        .. "; Path=/; Max-Age=" .. SESSION_TTL .. "; Secure; HttpOnly; SameSite=Lax"
    return true
end

local function valid_session(service)
    return validate_token(ngx.var["cookie_" .. COOKIE_NAME], "browser", service, SESSION_TTL, false)
end

local function valid_gateway_session()
    return validate_token(ngx.var["cookie_" .. GATEWAY_COOKIE_NAME], "gateway", "auth", SESSION_TTL, false)
end

local function issue_handoff_token(service)
    local current = load_secrets()
    if not current then return nil end
    local nonce_bytes = require("resty.random").bytes(18, true)
    if not nonce_bytes then
        ngx.log(ngx.ERR, "platform handoff nonce is unavailable")
        return nil
    end
    local now = ngx.time()
    local issued_at = tostring(now)
    local expires_at = tostring(now + HANDOFF_TTL)
    local nonce = base64url(nonce_bytes)
    local unsigned = unsigned_token("handoff", service, issued_at, expires_at, nonce)
    return unsigned .. "." .. signature(current, unsigned)
end

local function consume_url(target)
    return assert(target:match("^(https://[^/]+)")) .. "/platform-auth/consume"
end

function M.require_session(service)
    if not SERVICE_TARGETS[service] then
        ngx.log(ngx.ERR, "unknown platform service key")
        return ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
    end
    if valid_session(service) then return end
    if ngx.req.get_method() ~= "GET" and ngx.req.get_method() ~= "HEAD" then
        return ngx.exit(ngx.HTTP_UNAUTHORIZED)
    end
    return ngx.redirect(AUTH_ORIGIN .. "/login/" .. service, ngx.HTTP_MOVED_TEMPORARILY)
end

local function write_handoff(service)
    set_no_store_headers()
    local target = SERVICE_TARGETS[service]
    if not target then return ngx.exit(ngx.HTTP_NOT_FOUND) end
    if ngx.req.get_method() ~= "POST" then
        ngx.header["Allow"] = "POST"
        return ngx.exit(ngx.HTTP_NOT_ALLOWED)
    end
    local ticket = issue_handoff_token(service)
    if not ticket then return ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR) end
    ngx.header["Content-Type"] = "application/json; charset=utf-8"
    ngx.status = ngx.HTTP_OK
    ngx.say('{"consume":"' .. consume_url(target) .. '","redirect":"' .. target
        .. '","ticket":"' .. ticket .. '"}')
    return ngx.exit(ngx.HTTP_OK)
end

function M.issue_login_handoff(service)
    if not SERVICE_TARGETS[service] then return ngx.exit(ngx.HTTP_NOT_FOUND) end
    if ngx.req.get_method() ~= "POST" then
        ngx.header["Allow"] = "POST"
        return ngx.exit(ngx.HTTP_NOT_ALLOWED)
    end
    if not issue_cookie(GATEWAY_COOKIE_NAME, "gateway", "auth") then
        return ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
    end
    return write_handoff(service)
end

function M.issue_session_handoff(service)
    set_no_store_headers()
    if ngx.req.get_method() ~= "POST" then
        ngx.header["Allow"] = "POST"
        return ngx.exit(ngx.HTTP_NOT_ALLOWED)
    end
    if not SERVICE_TARGETS[service] then return ngx.exit(ngx.HTTP_NOT_FOUND) end
    if not valid_gateway_session() then
        return ngx.exit(ngx.HTTP_UNAUTHORIZED)
    end
    return write_handoff(service)
end

function M.consume_handoff(service)
    set_no_store_headers()
    local target = SERVICE_TARGETS[service]
    if not target then return ngx.exit(ngx.HTTP_NOT_FOUND) end
    if ngx.req.get_method() ~= "POST" then
        ngx.header["Allow"] = "POST"
        return ngx.exit(ngx.HTTP_NOT_ALLOWED)
    end
    ngx.req.read_body()
    local args, error_kind = ngx.req.get_post_args(2)
    if not args or error_kind == "truncated" or type(args.ticket) ~= "string" then
        return ngx.exit(ngx.HTTP_BAD_REQUEST)
    end
    local valid, replay_key = validate_token(args.ticket, "handoff", service, HANDOFF_TTL, true)
    if not valid then return ngx.exit(ngx.HTTP_UNAUTHORIZED) end
    local replay_cache = ngx.shared.sudox_platform_handoffs
    if not replay_cache or not replay_cache:add(replay_key, true, HANDOFF_TTL + CLOCK_SKEW) then
        return ngx.exit(ngx.HTTP_UNAUTHORIZED)
    end
    if not issue_cookie(COOKIE_NAME, "browser", service) then
        return ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
    end
    return ngx.redirect(target, ngx.HTTP_SEE_OTHER)
end

function M.logout()
    set_no_store_headers()
    if ngx.req.get_method() ~= "POST" then
        ngx.header["Allow"] = "POST"
        return ngx.exit(ngx.HTTP_NOT_ALLOWED)
    end
    ngx.header["Set-Cookie"] = COOKIE_NAME .. "=; Path=/; Max-Age=0; Secure; HttpOnly; SameSite=Lax"
    return ngx.exit(ngx.HTTP_NO_CONTENT)
end

function M.logout_gateway()
    set_no_store_headers()
    if ngx.req.get_method() ~= "POST" then
        ngx.header["Allow"] = "POST"
        return ngx.exit(ngx.HTTP_NOT_ALLOWED)
    end
    ngx.header["Set-Cookie"] = GATEWAY_COOKIE_NAME
        .. "=; Path=/; Max-Age=0; Secure; HttpOnly; SameSite=Lax"
    return ngx.exit(ngx.HTTP_NO_CONTENT)
end

function M.valid_session(service)
    return valid_session(service)
end

return M
