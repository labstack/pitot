import { useEffect, useState } from "react";
import type { Panel } from "../../../panels/types";
export const rowHeight = 40;
export const margin = 12;
export const pixels = (rows: number) => rows * rowHeight + (rows - 1) * margin;
// Mirrors the height tables in internal/dashboard/layout.go.
const heightRows = { s: 3, m: 6, l: 10 };
const minimumRows: Partial<Record<Panel["viz"], number>> = { stat: 4 };
export const defaultRows = (panel: Panel) => Math.max(minimumRows[panel.viz] ?? 0, heightRows[panel.height ?? (panel.viz === "service_map" ? "l" : panel.viz === "stat" ? "s" : "m")]);
/** The window's inner height, kept current across resizes. */
export function useWindowHeight() {
  const [height, setHeight] = useState(() => window.innerHeight);
  useEffect(() => {
    const resize = () => setHeight(window.innerHeight);
    window.addEventListener("resize", resize);
    return () => window.removeEventListener("resize", resize);
  }, []);
  return height;
}
export const fragmentPanelHeight = (panel: Panel) => Math.max(panel.viz === "service_map" ? 460 : 0, pixels(panel.grid?.h ?? defaultRows(panel)));
