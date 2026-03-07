import type { APIRoute } from "astro";

export const GET: APIRoute = ({ site }) => {
  const base = (site ?? new URL("https://scoutinggrade.com")).toString().replace(/\/$/, "");
  const body = [
    "User-agent: *",
    "Allow: /",
    "Content-signal: search=yes, ai-train=no, ai-input=no",
    "",
    "User-agent: GPTBot",
    "Disallow: /",
    "",
    "User-agent: ClaudeBot",
    "Disallow: /",
    "",
    "User-agent: Google-Extended",
    "Disallow: /",
    "",
    "User-agent: CCBot",
    "Disallow: /",
    "",
    "User-agent: Bytespider",
    "Disallow: /",
    "",
    "User-agent: Amazonbot",
    "Disallow: /",
    "",
    "User-agent: Applebot-Extended",
    "Disallow: /",
    "",
    `Sitemap: ${base}/sitemap.xml`,
    ""
  ].join("\n");

  return new Response(body, {
    headers: {
      "Content-Type": "text/plain; charset=utf-8"
    }
  });
};
