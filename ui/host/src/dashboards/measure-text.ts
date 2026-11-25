// One canvas serves every text measurement. A missing 2D context is not
// cached, so a canvas that becomes available later is still used.
let context: CanvasRenderingContext2D | null = null;
const canvasContext = () => context ??= document.createElement("canvas").getContext("2d");

/** Width of text in a CSS font, or undefined where canvas text metrics are unavailable. */
export function measureText(text: string, font: string): number | undefined {
  const ctx = canvasContext();
  if (!ctx) return undefined;
  ctx.font = font;
  return ctx.measureText(text).width;
}

/** A new measuring function on each call, so callers can key memos on it, or undefined without canvas. */
export function textMeasurer(): ((text: string, font: string) => number) | undefined {
  const ctx = canvasContext();
  return ctx ? (text, font) => { ctx.font = font; return ctx.measureText(text).width; } : undefined;
}
