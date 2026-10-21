local module_path = assert(arg[1], "module path required")
local legacy_module_path = assert(arg[2], "legacy module path required")
local now = 1000
local method = "POST"
local current_secret = string.rep("a", 64)
local previous_secret = nil
local response = {}
local post_args = {}
local replayed = {}

local real_open = io.open
io.open = function(path, mode)
    local value
    if path:match("quant%-platform%-session%-secret$") then
        value = current_secret
    elseif path:match("%.previous$") then
        value = previous_secret
    else
        value = current_secret
    end
    if not value then return nil, "not found" end
    return { read = function() return value end, close = function() end }
end

package.preload["resty.random"] = function()
    return { bytes = function() return "unique-handoff-nonce" end }
end

ngx = {
    ERR = "ERR",
    HTTP_OK = 200,
    HTTP_NO_CONTENT = 204,
    HTTP_SEE_OTHER = 303,
    HTTP_MOVED_TEMPORARILY = 302,
    HTTP_BAD_REQUEST = 400,
    HTTP_UNAUTHORIZED = 401,
    HTTP_NOT_FOUND = 404,
    HTTP_NOT_ALLOWED = 405,
    HTTP_INTERNAL_SERVER_ERROR = 500,
    var = {},
    header = {},
    shared = {
        sudox_platform_handoffs = {
            add = function(_, key)
                if replayed[key] then return false end
                replayed[key] = true
                return true
            end,
        },
    },
    req = {
        get_method = function() return method end,
        read_body = function() end,
        get_post_args = function() return post_args end,
    },
    time = function() return now end,
    encode_base64 = function(value) return (value:gsub("[^A-Za-z0-9]", "")) end,
    hmac_sha1 = function(secret, value) return secret .. value end,
    log = function(_, message) response.log = message end,
    say = function(value) response.body = value end,
    exit = function(status) response.status = status; return status end,
    redirect = function(target, status)
        response.redirect = target
        response.status = status
        return status
    end,
}

local function reset_response()
    response = {}
    ngx.header = {}
    ngx.status = nil
end

local auth = dofile(module_path)
assert(auth.issue_login_handoff("quant") == 200)
local gateway_header = assert(ngx.header["Set-Cookie"])
assert(gateway_header:match("^sudox_platform_gateway="))
assert(not gateway_header:match("Domain="))
local gateway_cookie = assert(gateway_header:match("^sudox_platform_gateway=([^;]+)"))
assert(response.body:match('"consume":"https://quant%.example%.com:11443/platform%-auth/consume"'))
assert(response.body:match('"redirect":"https://quant%.example%.com:11443/"'))
local ticket = assert(response.body:match('"ticket":"([^"]+)"'))

reset_response()
post_args = { ticket = ticket }
assert(auth.consume_handoff("quant") == 303)
assert(response.redirect == "https://quant.example.com:11443/")
local cookie_header = assert(ngx.header["Set-Cookie"])
assert(not cookie_header:match("Domain="))
assert(cookie_header:match("Secure; HttpOnly; SameSite=Lax"))
local cookie = assert(cookie_header:match("^sudox_platform_session=([^;]+)"))

-- The host-only gateway session can mint a second service handoff without
-- sending credentials again, but it is never exposed to sibling hosts.
ngx.var.cookie_sudox_platform_gateway = gateway_cookie
reset_response()
assert(auth.issue_session_handoff("stock") == 200)
assert(ngx.header["Set-Cookie"] == nil)
assert(response.body:match('"consume":"https://stock%-screener%.example%.com:11443/platform%-auth/consume"'))

ngx.var.cookie_sudox_platform_session = cookie
assert(auth.valid_session("quant"))
assert(not auth.valid_session("stock"))
reset_response()
method = "GET"
auth.require_session("stock")
assert(response.redirect == "https://auth.example.com:11443/login/stock")

reset_response()
method = "POST"
assert(auth.consume_handoff("quant") == 401)

ngx.var.cookie_sudox_platform_session = cookie:gsub("v3%.browser", "v3.machine", 1)
assert(not auth.valid_session("quant"))
ngx.var.cookie_sudox_platform_session = cookie
now = 1000 + 8 * 60 * 60
assert(not auth.valid_session("quant"))

now = 1000
current_secret = string.rep("b", 64)
previous_secret = string.rep("a", 64)
local rotated_auth = dofile(module_path)
ngx.var.cookie_sudox_platform_session = cookie
assert(rotated_auth.valid_session("quant"))

reset_response()
assert(rotated_auth.logout() == 204)
assert(ngx.header["Set-Cookie"]:match("Max%-Age=0"))
assert(not ngx.header["Set-Cookie"]:match("Domain="))

reset_response()
assert(rotated_auth.logout_gateway() == 204)
assert(ngx.header["Set-Cookie"]:match("^sudox_platform_gateway="))
assert(not ngx.header["Set-Cookie"]:match("Domain="))

-- A pre-migration consumer remains callable next to the migrated module.
current_secret = string.rep("c", 64)
previous_secret = nil
ngx.var.cookie_quant_platform_session = nil
method = "GET"
local legacy = dofile(legacy_module_path)
assert(type(legacy.set_session_cookie) == "function")
assert(type(legacy.require_session) == "function")
assert(type(legacy.issue_and_redirect) == "function")
reset_response()
legacy.require_session("/platform-auth/vibe")
assert(response.redirect == "https://quant.example.com:11443/platform-auth/vibe")

io.open = real_open
print("openresty auth harness: PASS")
