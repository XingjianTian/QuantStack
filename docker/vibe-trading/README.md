# Vibe-Trading downstream image

`Dockerfile.frontend-overlay` is for frontend-only integration releases. It
derives from the last validated full image and replaces only the production
`frontend/dist` tree. A backend or dependency change still requires rebuilding
the full Vibe-Trading Dockerfile.
