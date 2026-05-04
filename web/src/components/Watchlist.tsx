import { Search, Star } from "lucide-react";
import { useMemo, useState } from "react";

import type { TerminalAlert, TerminalTick } from "../domain/terminal";
import { formatCompact, formatCount, formatNumber, formatPct, formatTime } from "../lib/format";
import { Button } from "./ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "./ui/card";
import { Input } from "./ui/input";
import { ScrollArea } from "./ui/scroll-area";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./ui/table";

type WatchlistProps = {
  ticks: TerminalTick[];
  alerts: TerminalAlert[];
  selectedSymbol: string | null;
  onSelectSymbol: (symbol: string) => void;
};

export function Watchlist({ ticks, alerts, selectedSymbol, onSelectSymbol }: WatchlistProps) {
  const [query, setQuery] = useState("");
  const alertSymbols = useMemo(() => new Set(alerts.map((alert) => alert.symbol)), [alerts]);
  const visible = useMemo(() => {
    const needle = query.trim().toUpperCase();
    return ticks
      .filter((tick) => !needle || tick.symbol.includes(needle))
      .slice(0, 120);
  }, [query, ticks]);

  return (
    <Card className="terminal-panel watchlist-panel" aria-labelledby="watchlistTitle">
      <CardHeader className="panel-heading">
        <div>
          <p className="eyebrow">Universe</p>
          <CardTitle id="watchlistTitle">Live watchlist</CardTitle>
        </div>
        <div className="search-box">
          <Search size={15} aria-hidden="true" />
          <Input
            aria-label="Filter watchlist symbols"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Symbol"
            type="search"
          />
        </div>
      </CardHeader>

      <CardContent className="watchlist-content">
        <ScrollArea className="watchlist-scroll">
          {visible.length === 0 ? (
            <p className="empty-state">Waiting for ticks that match this filter.</p>
          ) : (
            <Table aria-label="Live tick table">
              <TableHeader>
                <TableRow>
                  <TableHead>Symbol</TableHead>
                  <TableHead>LTP</TableHead>
                  <TableHead>Chg</TableHead>
                  <TableHead>Bid/Ask</TableHead>
                  <TableHead>Volume</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {visible.map((tick) => {
                  const active = tick.symbol === selectedSymbol;
                  const hasAlert = alertSymbols.has(tick.symbol);
                  return (
                    <TableRow className={active ? "active" : ""} key={tick.instrument_key || tick.symbol}>
                      <TableCell>
                        <Button
                          className="symbol-button"
                          type="button"
                          variant="ghost"
                          onClick={() => onSelectSymbol(tick.symbol)}
                        >
                          <span className="symbol-cell">
                            {hasAlert && <Star size={13} aria-label="Has recent alert" />}
                            <strong>{tick.symbol}</strong>
                            <small>{formatTime(tick.ts)}</small>
                          </span>
                        </Button>
                      </TableCell>
                      <TableCell className="mono-cell">{formatNumber(tick.ltp)}</TableCell>
                      <TableCell className={Number(tick.change_pct ?? 0) >= 0 ? "positive mono-cell" : "negative mono-cell"}>
                        {formatPct(tick.change_pct)}
                      </TableCell>
                      <TableCell className="muted mono-cell">{formatNumber(tick.best_bid)} / {formatNumber(tick.best_ask)}</TableCell>
                      <TableCell className="mono-cell" title={formatCount(tick.volume_traded_today)}>{formatCompact(tick.volume_traded_today)}</TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          )}
        </ScrollArea>
      </CardContent>
    </Card>
  );
}
