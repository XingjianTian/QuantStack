local M = {}

local SECRET_PATH = "/opt/om/nginx/conf/quant-platform-session-secret"
local COOKIE_NAME = "quant_platform_session"
local SESSION_TTL = 8 * 60 * 60

local function read_secret()
    local file, open_error = io.open(SECRET_PATH, "rb")
    if not file then
        ngx.log(ngx.ERR, "cannot read Quant Platform session secret: ", open_error or "unknown error")
        return nil
    end
    local secret = file:read("*a")
    file:close()
    secret = (secret or ""):gsub("%s+$", "")
    if #secret < 32 then
        ngx.log(ngx.ERR, "Quant Platform session secret is missing or too short")
        return nil
    end
    return secret
end

local function base64url(value)
    return ngx.encode_base64(value):gsub("+", "-"):gsub("/", "_"):gsub("=+$", "")
end

local function signature(secret, expires_at)
    return base64url(ngx.hmac_sha1(secret, "v1." .. expires_at))
end

local function secure_equal(left, right)
    if type(left) ~= "string" or type(right) ~= "string" or #left ~= #right then
        return false
    end
    local bit = require("bit")
    local difference = 0
    for index = 1, #left do
        difference = bit.bor(difference, bit.bxor(left:byte(index), right:byte(index)))
    end
    return difference == 0
end

local function valid_session(secret)
    local token = ngx.var["cookie_" .. COOKIE_NAME]
    if not token then
        return false
    end
    local expires_at, supplied = token:match("^(%d+)%.([A-Za-z0-9_-]+)$")
    local expiry = tonumber(expires_at)
    local now = ngx.time()
    if not expiry or expiry < now or expiry > now + SESSION_TTL + 60 then
        return false
    end
    return secure_equal(supplied, signature(secret, expires_at))
end

function M.set_session_cookie()
    local secret = read_secret()
    if not secret then
        return false
    end
    if valid_session(secret) then
        return true
    end
    local expires_at = tostring(ngx.time() + SESSION_TTL)
    local token = expires_at .. "." .. signature(secret, expires_at)
    ngx.header["Set-Cookie"] = COOKIE_NAME .. "=" .. token
        .. "; Domain=.example.com; Path=/; Max-Age=" .. SESSION_TTL
        .. "; Secure; HttpOnly; SameSite=Lax"
    return true
end

function M.require_session(bridge_path)
    local secret = read_secret()
    if secret and valid_session(secret) then
        return
    end
    if ngx.req.get_method() ~= "GET" and ngx.req.get_method() ~= "HEAD" then
        return ngx.exit(ngx.HTTP_UNAUTHORIZED)
    end
    return ngx.redirect("https://quant.example.com:11443" .. bridge_path, ngx.HTTP_MOVED_TEMPORARILY)
end

function M.issue_and_redirect(target)
    if not M.set_session_cookie() then
        return ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
    end
    return ngx.redirect(target, ngx.HTTP_MOVED_TEMPORARILY)
end

return M
