import { useEffect, useState } from "react";

export type PlatformTheme = "dark" | "light";

const cookieName = "quant-platform-theme";
const storageKey = "quant-platform-theme";
const eventName = "quant-platform-themechange";

function readStorage(): string | null {
  try {
    return typeof window.localStorage?.getItem === "function" ? window.localStorage.getItem(storageKey) : null;
  } catch {
    return null;
  }
}

function writeStorage(theme: PlatformTheme) {
  try {
    if (typeof window.localStorage?.setItem === "function") window.localStorage.setItem(storageKey, theme);
  } catch {
    // Cookie synchronization remains available when storage is blocked.
  }
}

function validTheme(value: string | null | undefined): value is PlatformTheme {
  return value === "dark" || value === "light";
}

function readCookie(): PlatformTheme | null {
  const value = document.cookie
    .split(";")
    .map((part) => part.trim())
    .find((part) => part.startsWith(`${cookieName}=`))
    ?.slice(cookieName.length + 1);
  return validTheme(value) ? value : null;
}

function cookieDomain(): string {
  const host = window.location.hostname;
  if (host === "localhost" || host === "127.0.0.1" || !host.includes(".")) return "";
  return `; Domain=.${host.replace(/^[^.]+\./, "")}`;
}

export function readPlatformTheme(): PlatformTheme {
  const cookie = readCookie();
  if (cookie) return cookie;
  const stored = readStorage();
  return validTheme(stored) ? stored : "dark";
}

function persistPlatformTheme(theme: PlatformTheme) {
  writeStorage(theme);
  const secure = window.location.protocol === "https:" ? "; Secure" : "";
  document.cookie = `${cookieName}=${theme}; Path=/; Max-Age=31536000; SameSite=Lax${secure}${cookieDomain()}`;
  window.dispatchEvent(new CustomEvent<PlatformTheme>(eventName, { detail: theme }));
}

export function usePlatformTheme() {
  const [theme, setTheme] = useState<PlatformTheme>(readPlatformTheme);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    document.documentElement.style.colorScheme = theme;
    persistPlatformTheme(theme);
  }, [theme]);

  useEffect(() => {
    const sync = () => {
      const next = readPlatformTheme();
      setTheme((current) => current === next ? current : next);
    };
    const onThemeChange = (event: Event) => {
      const next = (event as CustomEvent<PlatformTheme>).detail;
      if (validTheme(next)) setTheme(next);
    };
    const onVisibility = () => { if (!document.hidden) sync(); };
    const interval = window.setInterval(sync, 800);
    window.addEventListener("focus", sync);
    window.addEventListener("storage", sync);
    window.addEventListener(eventName, onThemeChange);
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      window.clearInterval(interval);
      window.removeEventListener("focus", sync);
      window.removeEventListener("storage", sync);
      window.removeEventListener(eventName, onThemeChange);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, []);

  return {
    theme,
    toggleTheme: () => setTheme((current) => current === "dark" ? "light" : "dark"),
  };
}
