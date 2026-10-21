ARG UPSTREAM_IMAGE=ghcr.io/xang1234/stockscreenclaude-frontend:v1.4.0@sha256:b738f2c18172b9c12d53e13ff92bf38f59145ec1154d9cc5a6fa98c4a4f1c23b
FROM ${UPSTREAM_IMAGE}

COPY quant-center.svg quant-theme-init.js quant-theme-init.html quant-zh-cn.js /usr/share/nginx/html/

# Keep the official application bundle byte-for-byte intact. The downstream
# localization layer is loaded after the application entry point and also
# observes React portals and asynchronous updates.
RUN awk '/<\/head>/{while ((getline line < "/usr/share/nginx/html/quant-theme-init.html") > 0) print line} {print}' /usr/share/nginx/html/index.html > /tmp/index.html \
    && mv /tmp/index.html /usr/share/nginx/html/index.html \
    && sed -i 's#<title>[^<]*</title>#<title>Quant Center</title>#' /usr/share/nginx/html/index.html \
    && sed -i 's#href="/fire.svg"#href="/quant-center.svg"#' /usr/share/nginx/html/index.html \
    && sed -i 's#</body>#<script src="/quant-zh-cn.js"></script></body>#' /usr/share/nginx/html/index.html \
    && sed -i "s#script-src 'self' https://s3.tradingview.com;#script-src 'self' 'sha256-tuvzR0DlMF8dx6Aj8dNOeZHF1JpWOnBWwYeEVCNbiw8=' https://s3.tradingview.com;#" /etc/nginx/conf.d/default.conf \
    && sed -i '/    # SPA routing/i\
    # The HTML shell and downstream localization layer must be revalidated on\
    # every visit. Hashed upstream assets remain immutable for one year.\
    location = / {\
        add_header Cache-Control "no-store, no-cache, must-revalidate" always;\
        try_files /index.html =404;\
    }\
\
    location = /index.html {\
        add_header Cache-Control "no-store, no-cache, must-revalidate" always;\
        try_files $uri =404;\
    }\
\
    location = /quant-zh-cn.js {\
        add_header Cache-Control "no-store, no-cache, must-revalidate" always;\
        try_files $uri =404;\
    }\
\
    location = /quant-center.svg {\
        expires 1y;\
        add_header Cache-Control "public, immutable";\
        try_files $uri =404;\
    }\
' /etc/nginx/conf.d/default.conf
