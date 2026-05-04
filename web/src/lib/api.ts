import type { TerminalState } from "../domain/terminal";

const TERMINAL_STATE_ENDPOINT = "/api/terminal/state";

type FetchTerminalStateOptions = {
  endpoint?: string;
  fetcher?: typeof fetch;
  signal?: AbortSignal;
};

class TerminalApiError extends Error {
  readonly status?: number;
  readonly cause?: unknown;

  constructor(message: string, options: { status?: number; cause?: unknown } = {}) {
    super(message);
    this.name = "TerminalApiError";
    this.status = options.status;
    this.cause = options.cause;
  }
}

export async function fetchTerminalState(options: FetchTerminalStateOptions = {}): Promise<TerminalState> {
  const endpoint = options.endpoint ?? TERMINAL_STATE_ENDPOINT;
  const request = options.fetcher ?? globalThis.fetch;

  if (!request) {
    throw new TerminalApiError("This browser cannot load the live terminal because fetch is unavailable.");
  }

  let response: Response;
  try {
    response = await request(endpoint, {
      cache: "no-store",
      headers: {
        Accept: "application/json",
      },
      signal: options.signal,
    });
  } catch (error) {
    if (isAbortError(error)) {
      throw new TerminalApiError("Terminal state request was cancelled.", { cause: error });
    }
    throw new TerminalApiError("Could not reach the live terminal. Check that the liq-sweep server is running.", {
      cause: error,
    });
  }

  const payload = await readJsonBody(response);
  if (!response.ok) {
    throw new TerminalApiError(errorMessageForStatus(response.status, payload), { status: response.status });
  }

  return payload as TerminalState;
}

async function readJsonBody(response: Response): Promise<unknown> {
  try {
    return await response.json();
  } catch {
    return null;
  }
}

function errorMessageForStatus(status: number, payload: unknown): string {
  const serverMessage = extractServerError(payload);
  if (serverMessage) return serverMessage;
  if (status === 404) return "The live terminal endpoint was not found. Restart the liq-sweep server and try again.";
  if (status >= 500) return "The live terminal server had a problem while building state. Try again shortly.";
  return `The live terminal returned HTTP ${status}.`;
}

function extractServerError(payload: unknown): string | null {
  if (!payload || typeof payload !== "object") return null;
  const error = (payload as { error?: unknown }).error;
  return typeof error === "string" && error.trim() ? error : null;
}

function isAbortError(error: unknown): boolean {
  return typeof DOMException !== "undefined" && error instanceof DOMException && error.name === "AbortError";
}
