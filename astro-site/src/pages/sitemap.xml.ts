import type { APIRoute } from "astro";
import { statSync } from "node:fs";
import meta from "../data/build_meta.json";
import board from "../data/big_board_2026.json";
import teamNeeds from "../data/team_needs_2026.json";
import { teamPath } from "../lib/teamNeeds";

const ROUTES = [
  "/",
  "/2026-nfl-draft-big-board",
  "/2026-nfl-mock-draft",
  "/2026-nfl-mock-draft-round-1",
  "/2026-nfl-7-round-mock-draft",
  "/2026-nfl-player-comparison",
  "/nfl-team-needs-2026",
  "/scouting-cards",
  "/nfl-draft-methodology",
  "/2026-nfl-draft-weekly-updates"
];

function xmlEscape(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function safeMtime(path: URL): string | null {
  try {
    return statSync(path).mtime.toISOString();
  } catch {
    return null;
  }
}

const generatedAt = (meta as { generated_at?: string }).generated_at || new Date().toISOString();
const routeLastmods: Record<string, string> = {
  "/": safeMtime(new URL("./index.astro", import.meta.url)) || generatedAt,
  "/2026-nfl-draft-big-board": generatedAt,
  "/2026-nfl-mock-draft": generatedAt,
  "/2026-nfl-mock-draft-round-1": generatedAt,
  "/2026-nfl-7-round-mock-draft": generatedAt,
  "/2026-nfl-player-comparison": generatedAt,
  "/nfl-team-needs-2026": generatedAt,
  "/scouting-cards": generatedAt,
  "/nfl-draft-methodology": safeMtime(new URL("./nfl-draft-methodology.astro", import.meta.url)) || generatedAt,
  "/2026-nfl-draft-weekly-updates": generatedAt,
};

export const GET: APIRoute = ({ site }) => {
  const base = (site ?? new URL("https://scoutinggrade.com")).toString().replace(/\/$/, "");
  const playerRoutes = (board as any[])
    .map((p: any) => p?.slug ? `/players/${p.slug}` : "")
    .filter(Boolean);
  const teamRoutes = (teamNeeds as any[])
    .map((row: any) => row?.team ? teamPath(row.team) : "")
    .filter(Boolean);
  const allRoutes = [...ROUTES, ...playerRoutes, ...teamRoutes];

  const urls = allRoutes.map((path) => {
    const loc = `${base}${path === "/" ? "/" : path}`;
    const lastmod = routeLastmods[path] || generatedAt;
    return [
      "  <url>",
      `    <loc>${xmlEscape(loc)}</loc>`,
      `    <lastmod>${xmlEscape(lastmod)}</lastmod>`,
      "  </url>"
    ].join("\n");
  }).join("\n");

  const body = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    urls,
    "</urlset>",
    ""
  ].join("\n");

  return new Response(body, {
    headers: {
      "Content-Type": "application/xml; charset=utf-8"
    }
  });
};
