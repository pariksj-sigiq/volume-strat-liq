const LOCALE = "en-IN";
const MARKET_TIME_ZONE = "Asia/Kolkata";
const EMPTY_VALUE = "-";

type NumericInput = number | string | null | undefined;

type NumberFormatOptions = number | {
  minimumFractionDigits?: number;
  maximumFractionDigits?: number;
  signed?: boolean;
};

type TimeFormatOptions = {
  includeDate?: boolean;
  includeSeconds?: boolean;
  timeZone?: string;
};

export function formatNumber(value: NumericInput, options: NumberFormatOptions = {}): string {
  const number = finiteNumber(value);
  if (number === null) return EMPTY_VALUE;
  const digits = normalizeNumberFormatOptions(options, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  return new Intl.NumberFormat(LOCALE, {
    minimumFractionDigits: digits.minimumFractionDigits,
    maximumFractionDigits: digits.maximumFractionDigits,
    signDisplay: digits.signed ? "always" : "auto",
  }).format(number);
}

export function formatCount(value: NumericInput): string {
  const number = finiteNumber(value);
  if (number === null) return EMPTY_VALUE;
  return new Intl.NumberFormat(LOCALE, {
    maximumFractionDigits: 0,
  }).format(Math.round(number));
}

export function formatPct(value: NumericInput, options: NumberFormatOptions = {}): string {
  const number = finiteNumber(value);
  if (number === null) return EMPTY_VALUE;
  const digits = normalizeNumberFormatOptions(options, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
    signed: true,
  });
  return `${new Intl.NumberFormat(LOCALE, {
    minimumFractionDigits: digits.minimumFractionDigits,
    maximumFractionDigits: digits.maximumFractionDigits,
    signDisplay: digits.signed ? "always" : "auto",
  }).format(number)}%`;
}

export function formatTime(value: string | Date | null | undefined, options: TimeFormatOptions = {}): string {
  if (!value) return EMPTY_VALUE;
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return EMPTY_VALUE;
  return new Intl.DateTimeFormat(LOCALE, {
    ...(options.includeDate ? { day: "2-digit", month: "short", year: "numeric" } : {}),
    hour: "2-digit",
    minute: "2-digit",
    ...(options.includeSeconds ?? true ? { second: "2-digit" } : {}),
    hour12: false,
    timeZone: options.timeZone ?? MARKET_TIME_ZONE,
  }).format(date);
}

export function formatCompact(value: NumericInput, options: NumberFormatOptions = {}): string {
  const number = finiteNumber(value);
  if (number === null) return EMPTY_VALUE;
  const digits = normalizeNumberFormatOptions(options, { minimumFractionDigits: 0, maximumFractionDigits: 1 });
  return new Intl.NumberFormat(LOCALE, {
    notation: "compact",
    compactDisplay: "short",
    minimumFractionDigits: digits.minimumFractionDigits,
    maximumFractionDigits: digits.maximumFractionDigits,
    signDisplay: digits.signed ? "always" : "auto",
  }).format(number);
}

function finiteNumber(value: NumericInput): number | null {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function normalizeNumberFormatOptions(
  options: NumberFormatOptions,
  defaults: Required<Pick<Exclude<NumberFormatOptions, number>, "minimumFractionDigits" | "maximumFractionDigits">> &
    Pick<Exclude<NumberFormatOptions, number>, "signed">,
) {
  if (typeof options === "number") {
    return {
      minimumFractionDigits: options,
      maximumFractionDigits: options,
      signed: defaults.signed ?? false,
    };
  }
  return {
    minimumFractionDigits: options.minimumFractionDigits ?? defaults.minimumFractionDigits,
    maximumFractionDigits: options.maximumFractionDigits ?? defaults.maximumFractionDigits,
    signed: options.signed ?? defaults.signed ?? false,
  };
}
