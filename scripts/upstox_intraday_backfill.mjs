import { runIntradayCli } from "../src/upstox/backfill.mjs";

runIntradayCli().catch((error) => {
  console.error(error instanceof Error ? error.stack : error);
  process.exitCode = 1;
});
