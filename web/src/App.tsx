import { useEffect, useMemo, useState } from "react";

import { AlertInbox } from "./components/AlertInbox";
import { SectionCards } from "./components/SectionCards";
import { StatusStrip } from "./components/StatusStrip";
import { SymbolInspector } from "./components/SymbolInspector";
import { TerminalRail } from "./components/TerminalRail";
import { TerminalSidebar } from "./components/TerminalSidebar";
import { ToastStack } from "./components/ToastStack";
import { Watchlist } from "./components/Watchlist";
import { Tabs, TabsList, TabsTrigger } from "./components/ui/tabs";
import { useTerminalState } from "./hooks/useTerminalState";

type MobileView = "tape" | "alerts" | "symbol" | "system";

export default function App() {
  const {
    snapshot,
    loadError,
    toasts,
    tickHistory,
    browserAlertsEnabled,
    defaultSymbol,
    enableBrowserAlerts,
    removeToast,
  } = useTerminalState();

  const [selectedSymbol, setSelectedSymbol] = useState<string | null>(null);
  const [mobileView, setMobileView] = useState<MobileView>("tape");

  useEffect(() => {
    if (!selectedSymbol && defaultSymbol) {
      setSelectedSymbol(defaultSymbol);
    }
  }, [defaultSymbol, selectedSymbol]);

  const selectedTick = useMemo(
    () => snapshot?.ticks.find((tick) => tick.symbol === selectedSymbol) ?? null,
    [selectedSymbol, snapshot?.ticks],
  );
  const selectedAlert = useMemo(
    () => snapshot?.alerts.find((alert) => alert.symbol === selectedSymbol) ?? null,
    [selectedSymbol, snapshot?.alerts],
  );
  const selectedHistory = selectedSymbol ? tickHistory[selectedSymbol] ?? [] : [];

  const selectSymbol = (symbol: string) => {
    setSelectedSymbol(symbol);
    setMobileView("symbol");
  };

  return (
    <div className="terminal-app">
      <div className="terminal-shell">
        <TerminalRail snapshot={snapshot} />

        <div className="terminal-main">
          <StatusStrip
            snapshot={snapshot}
            loadError={loadError}
            browserAlertsEnabled={browserAlertsEnabled}
            onEnableAlerts={enableBrowserAlerts}
          />

          <SectionCards snapshot={snapshot} />

          <Tabs className="mobile-switcher" value={mobileView} onValueChange={(value) => setMobileView(value as MobileView)}>
            <TabsList aria-label="Terminal panels">
              <TabsTrigger value="tape">Tape</TabsTrigger>
              <TabsTrigger value="alerts">Alerts</TabsTrigger>
              <TabsTrigger value="symbol">Symbol</TabsTrigger>
              <TabsTrigger value="system">System</TabsTrigger>
            </TabsList>
          </Tabs>

          <main className="terminal-workspace">
            <div className={`workspace-column tape-column ${mobileView === "tape" ? "mobile-active" : ""}`}>
              <Watchlist
                ticks={snapshot?.ticks ?? []}
                alerts={snapshot?.alerts ?? []}
                selectedSymbol={selectedSymbol}
                onSelectSymbol={selectSymbol}
              />
            </div>

            <div className={`workspace-column center-column ${mobileView === "alerts" || mobileView === "symbol" ? "mobile-active" : ""}`}>
              <div className={mobileView === "symbol" ? "mobile-hidden" : ""}>
                <AlertInbox
                  alerts={snapshot?.alerts ?? []}
                  snapshot={snapshot}
                  selectedSymbol={selectedSymbol}
                  onSelectSymbol={selectSymbol}
                />
              </div>
              <div className={mobileView === "alerts" ? "mobile-hidden" : ""}>
                <SymbolInspector
                  selectedSymbol={selectedSymbol}
                  tick={selectedTick}
                  alert={selectedAlert}
                  history={selectedHistory}
                />
              </div>
            </div>

            <div className={`workspace-column side-column ${mobileView === "system" ? "mobile-active" : ""}`}>
              <TerminalSidebar snapshot={snapshot} />
            </div>
          </main>
        </div>
      </div>

      <ToastStack toasts={toasts} onDismiss={removeToast} />
    </div>
  );
}
