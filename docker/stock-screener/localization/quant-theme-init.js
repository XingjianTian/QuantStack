(() => {
  // Keep the anti-flash guard bounded. The downstream bridge still settles
  // the platform theme after React mounts, but a slow cold asset or gateway
  // response must not hide a usable application for seconds.
  const themeBootstrapMaxDelayMs = 500;
  const cookieName = 'quant-platform-theme';
  const value = document.cookie.split(';').map((part) => part.trim())
    .find((part) => part.startsWith(`${cookieName}=`))
    ?.slice(cookieName.length + 1);
  const theme = value === 'dark' || value === 'light'
    ? value
    : window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  const root = document.documentElement;
  root.dataset.quantPlatformTheme = theme;
  root.classList.add('quant-theme-pending');
  root.style.colorScheme = theme;

  const background = theme === 'dark' ? '#0c0c11' : '#f5f5f5';
  const style = document.createElement('style');
  style.id = 'quant-theme-bootstrap';
  style.textContent = `
    html, body { background: ${background}; }
    html.quant-theme-pending #root { visibility: hidden; }
  `;
  document.head.appendChild(style);

  // Never leave the application hidden if an upstream upgrade prevents the
  // downstream bridge from finding its native theme control.
  window.setTimeout(() => root.classList.remove('quant-theme-pending'), themeBootstrapMaxDelayMs);
})();
