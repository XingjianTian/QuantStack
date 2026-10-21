# Include inside each protected UI server after setting $sudox_platform_service.
location = /platform-auth/consume {
    if ($request_method != POST) { return 405; }
    content_by_lua_block {
        require("sudox-platform-auth").consume_handoff(ngx.var.sudox_platform_service)
    }
}

location = /platform-auth/logout {
    if ($request_method != POST) { return 405; }
    content_by_lua_block {
        require("sudox-platform-auth").logout()
    }
}
