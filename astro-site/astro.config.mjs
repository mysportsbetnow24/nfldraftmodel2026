import { defineConfig } from "astro/config";

export default defineConfig({
  site: "https://www.scoutinggrade.com",
  output: "static",
  build: {
    format: "file"
  }
});
