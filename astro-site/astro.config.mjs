import { defineConfig } from "astro/config";

export default defineConfig({
  site: "https://scoutinggrade.com",
  output: "static",
  build: {
    format: "file"
  }
});
