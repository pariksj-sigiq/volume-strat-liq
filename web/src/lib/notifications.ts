import type { TerminalAlert } from "../domain/terminal";
import { formatNumber } from "./format";

type AlertIdentity = Pick<TerminalAlert, "symbol" | "generated_at" | "entry" | "profile_name">;

type AlertMessage = {
  title: string;
  body: string;
};

type NotificationOptions = {
  notificationApi?: typeof Notification;
};

type BrowserNotificationOptions = NotificationOptions & {
  notifyInApp?: (alert: TerminalAlert, message: AlertMessage) => void;
  createMessage?: (alert: TerminalAlert) => AlertMessage;
};

const notifiedAlertIds = new Set<string>();

export function createAlertId(alert: AlertIdentity): string {
  return [alert.symbol, alert.profile_name, alert.generated_at, alert.entry].join(":");
}

export async function requestAlertNotificationPermission(options: NotificationOptions = {}): Promise<boolean> {
  const notificationApi = options.notificationApi ?? globalThis.Notification;
  if (!notificationApi) return false;
  if (notificationApi.permission === "granted") return true;
  if (notificationApi.permission === "denied") return false;
  return (await notificationApi.requestPermission()) === "granted";
}

export function shouldNotifyAlert(seenAlertIds: Set<string>, alert: AlertIdentity): boolean;
export function shouldNotifyAlert(alert: AlertIdentity, seenAlertIds?: Set<string>): boolean;
export function shouldNotifyAlert(
  first: AlertIdentity | Set<string>,
  second: AlertIdentity | Set<string> = notifiedAlertIds,
): boolean {
  const seenAlertIds = first instanceof Set ? first : second instanceof Set ? second : notifiedAlertIds;
  const alert = first instanceof Set ? second : first;
  if (alert instanceof Set) return false;
  const alertId = createAlertId(alert);
  if (seenAlertIds.has(alertId)) return false;
  seenAlertIds.add(alertId);
  return true;
}

export function sendBrowserAlertNotification(alert: TerminalAlert, options: BrowserNotificationOptions = {}): boolean {
  const message = (options.createMessage ?? createDefaultMessage)(alert);
  options.notifyInApp?.(alert, message);

  const notificationApi = options.notificationApi ?? globalThis.Notification;
  if (!notificationApi || notificationApi.permission !== "granted") return false;

  new notificationApi(message.title, { body: message.body });
  return true;
}

function createDefaultMessage(alert: TerminalAlert): AlertMessage {
  return {
    title: `${alert.symbol} ${alert.profile_label}`,
    body: `Entry ${formatNumber(alert.entry)} · SL ${formatNumber(alert.sl)} · TP ${formatNumber(alert.tp)}`,
  };
}
