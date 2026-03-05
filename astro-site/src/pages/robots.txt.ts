import type { APIRoute } from "astro";

export const GET: APIRoute = ({ site }) => {
  const base = (site ?? new URL("https://scoutinggrade.com")).toString().replace(/\/$/, "");
  const body = [
    "User-agent: *",
    "Allow: /",
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

