import { runCli } from "../src/upstox/backfill.mjs";

runCli().catch((error) => {
  console.error(error instanceof Error ? error.stack : error);
  process.exitCode = 1;
});
