type IsoDateTimeString = string;

export interface TerminalTick {
  symbol: string;
  instrument_key: string;
  ts: IsoDateTimeString;
  ltp: number;
  close_price: number | null;
  change_pct: number | null;
  last_quantity: number | null;
  volume_traded_today: number | null;
  open_interest: number | null;
  best_bid: number | null;
  best_ask: number | null;
}

export interface TerminalAlert {
  symbol: string;
  sector: string;
  entry: number;
  sl: number;
  tp: number;
  risk_reward: string;
  volume_multiple: number;
  reason: string;
  generated_at: IsoDateTimeString;
  profile_name: string;
  profile_label: string;
}

export interface TerminalSignalBlock {
  symbol: string;
  reason: string;
  bar_ts: IsoDateTimeString;
}

type TerminalConnectionEvent = {
  type: "connection";
  connected: boolean;
  mode: string;
  error: string | null;
  ts: IsoDateTimeString;
};

type TerminalSignalStatusEvent = {
  type: "signal_status";
  enabled: boolean;
  mode: string;
  reason: string;
  ts: IsoDateTimeString;
};

type TerminalWarmupStatusEvent = {
  type: "warmup_status";
  seed_count: number;
  required_count: number;
  reason: string;
  ts: IsoDateTimeString;
};

type TerminalAlertEvent = TerminalAlert & {
  type: "alert";
};

type TerminalUnknownEvent = {
  type: string;
  [key: string]: unknown;
};

export type TerminalEvent =
  | TerminalAlertEvent
  | TerminalConnectionEvent
  | TerminalSignalStatusEvent
  | TerminalWarmupStatusEvent
  | TerminalUnknownEvent;

export interface TerminalState {
  ok: boolean;
  connected: boolean;
  feed_mode: string;
  universe_count: number;
  tick_count: number;
  subscribed_instrument_count: number;
  active_instrument_count: number;
  ticks_total: number;
  tick_rate_per_min: number;
  bar_interval: string;
  min_bars_for_signal: number;
  alert_count: number;
  last_error: string | null;
  last_tick_at: IsoDateTimeString | null;
  signals_enabled: boolean;
  signal_mode: string;
  signal_status_reason: string;
  baseline_count: number;
  baseline_required: number;
  warmup_seed_count: number;
  warmup_required_count: number;
  warmup_status_reason: string;
  signal_block_counts: Record<string, number>;
  latest_signal_blocks: TerminalSignalBlock[];
  server_time: IsoDateTimeString;
  ticks: TerminalTick[];
  alerts: TerminalAlert[];
  events: TerminalEvent[];
}
