import { writeFile } from "node:fs/promises";
import openapiTS from "openapi-typescript";

const FALLBACK_BASE = "http://127.0.0.1:8765";
const EXPECTED_PATHS = ["/api/props", "/api/board/summary", "/api/startup/snapshot"];

function resolveBaseUrl() {
  const base = process.env.OPENAPI_BASE_URL || process.env.VITE_API_BASE || FALLBACK_BASE;
  return base.replace(/\/$/, "");
}

async function main() {
  const baseUrl = resolveBaseUrl();
  const schemaUrl = `${baseUrl}/openapi.json`;

  const response = await fetch(schemaUrl);
  if (!response.ok) {
    throw new Error(`Failed to fetch OpenAPI schema from ${schemaUrl}: ${response.status}`);
  }
  const schema = await response.json();
  const availablePaths = schema?.paths ?? {};
  const missing = EXPECTED_PATHS.filter((path) => !(path in availablePaths));
  if (missing.length > 0) {
    throw new Error(
      `OpenAPI schema at ${schemaUrl} is missing expected app paths: ${missing.join(", ")}`
    );
  }

  const output = await openapiTS(schema);
  await writeFile("src/api/schema.ts", output, "utf8");
  // eslint-disable-next-line no-console
  console.log(`Generated src/api/schema.ts from ${schemaUrl}`);
}

main().catch((error) => {
  // eslint-disable-next-line no-console
  console.error(error);
  process.exit(1);
});
