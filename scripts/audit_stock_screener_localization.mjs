#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(scriptDir, "..");
const sourceRoot = path.resolve(
  process.argv[2] ?? process.env.STOCK_SCREENER_FRONTEND_ROOT ?? path.join(projectRoot, "../upstream/stock-screener/frontend/src"),
);
const localizerPath = path.join(projectRoot, "docker/stock-screener/localization/quant-zh-cn.js");

if (!fs.existsSync(sourceRoot)) {
  console.error(`Stock Screener frontend source not found: ${sourceRoot}`);
  process.exit(2);
}

const professionalTerms = new Set([
  "% vs EMA10", "% vs EMA20", "% vs EMA50", "CSI 300", "FTSE Bursa Malaysia KLCI",
  "Grp Rnk", "Hang Seng Index", "KOSPI Composite", "MCap", "NIFTY 50", "Nikkei 225",
  "Pvt", "Pvt%", "Rate", "Rdy", "RS vs SPY", "RS Δ", "S&P/ASX 200",
  "S&P/TSX Composite", "SE Blue Dot", "SE Pattern", "SE Ready", "SE Score", "Score Δ",
  "Src", "Straits Times Index", "Tails", "U/D Vol", "Vol BT", "Vol/50d", "vs EMA10",
  "vs EMA20", "vs EMA50", "VScr", "β-adj RS", "βRS",
]);

const falsePositivePatterns = [
  /^\(prev$/,
  /^\/ 100 exposure$/,
  /^0 [?>&]/,
  /^[ab](?:Val)? \?/,
  /^right \?/,
  /^1 \|\|/,
  /^and$/,
];

function walk(directory) {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const target = path.join(directory, entry.name);
    if (entry.isDirectory()) return walk(target);
    if (!/\.jsx?$/.test(entry.name) || entry.name.includes(".test.")) return [];
    return [target];
  });
}

function normalize(value) {
  return value
    .replace(/&quot;/g, '"')
    .replace(/&amp;/g, "&")
    .replace(/&gt;/g, ">")
    .replace(/&lt;/g, "<")
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, " ")
    .trim();
}

function isCandidate(value) {
  return value.length >= 2
    && value.length <= 180
    && /[A-Za-z]{2}/.test(value)
    && !/[{}=<>]/.test(value)
    && !/^https?:|^[a-z]+\.[a-z]|^[A-Z0-9_./:-]+$/.test(value);
}

const localizer = fs.readFileSync(localizerPath, "utf8");
const translated = new Set(
  [...localizer.matchAll(/^\s*'((?:[^'\\]|\\.)+)':/gm)].map((match) => match[1]),
);
const candidates = new Map();

function record(raw, filename) {
  const value = normalize(raw);
  if (!isCandidate(value)) return;
  if (!candidates.has(value)) candidates.set(value, new Set());
  candidates.get(value).add(path.relative(sourceRoot, filename));
}

for (const filename of walk(sourceRoot)) {
  const source = fs.readFileSync(filename, "utf8");
  // JSX copy is often formatted on its own line between opening and closing
  // tags. Accept whitespace/newlines while still refusing nested tags or
  // expression braces, otherwise the audit silently misses visible labels.
  for (const match of source.matchAll(/<[A-Za-z][^>]*>\s*([^<>{}]*[A-Za-z][^<>{}]*)\s*<\//g)) record(match[1], filename);
  for (const match of source.matchAll(/(?:label|title|placeholder|aria-label|helperText|primary|secondary)\s*=\s*["']([^"']+)["']/g)) record(match[1], filename);
  for (const match of source.matchAll(/(?:label|title|description|message|caption|heading|subheader|tooltip)\s*:\s*["'`]([^"'`$]+)["'`]/g)) record(match[1], filename);
}

const missing = [...candidates.entries()]
  .filter(([value]) => !translated.has(value))
  .filter(([value]) => !professionalTerms.has(value))
  .filter(([value]) => !falsePositivePatterns.some((pattern) => pattern.test(value)))
  .sort(([left], [right]) => left.localeCompare(right));

console.log(`Stock Screener UI literals: ${candidates.size}`);
console.log(`Covered or explicitly preserved: ${candidates.size - missing.length}`);
console.log(`Actionable untranslated literals: ${missing.length}`);
for (const [value, files] of missing) {
  console.log(`- ${value}  [${[...files].slice(0, 3).join(", ")}]`);
}

process.exitCode = missing.length === 0 ? 0 : 1;
