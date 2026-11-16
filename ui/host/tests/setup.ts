// Node >= 22 defines an experimental globalThis.localStorage getter that returns undefined
// (no --localstorage-file) and shadows happy-dom's Storage; replace it with a real one.
if (typeof window !== "undefined" && !globalThis.localStorage) {
  const { Storage } = window as unknown as { Storage: new () => Storage };
  Object.defineProperty(globalThis, "localStorage", { value: new Storage(), configurable: true });
}

// happy-dom has no FontFaceSet, so document.fonts is undefined. Mantine's
// autosize textarea subscribes to its "loadingdone" event, which throws on
// mount without this; an EventTarget that never fires is enough.
if (typeof document !== "undefined" && !document.fonts) {
  Object.defineProperty(document, "fonts", { value: new EventTarget(), configurable: true });
}

// Browsers queue `selectionchange` as an asynchronous task; happy-dom fires it
// synchronously inside the Selection mutation. CodeMirror writes the DOM
// selection while applying a view update, and @codemirror/view 6.43.15 rejects
// the reentrant update that a synchronous selectionchange then triggers from its
// DOM observer. Defer selectionchange listeners to a microtask so the editor
// sees the same ordering it does in a real browser.
if (typeof document !== "undefined") {
  const target = document as unknown as EventTarget;
  const add = target.addEventListener.bind(target);
  const remove = target.removeEventListener.bind(target);
  const wrappers = new WeakMap<EventListenerOrEventListenerObject, EventListener>();
  const invoke = (listener: EventListenerOrEventListenerObject, event: Event) =>
    (typeof listener === "function" ? listener : listener.handleEvent).call(document, event);
  target.addEventListener = (type, listener, options) => {
    if (type !== "selectionchange" || !listener) return add(type, listener, options);
    const wrapped: EventListener = event => queueMicrotask(() => invoke(listener, event));
    wrappers.set(listener, wrapped);
    return add(type, wrapped, options);
  };
  target.removeEventListener = (type, listener, options) => {
    if (type !== "selectionchange" || !listener) return remove(type, listener, options);
    return remove(type, wrappers.get(listener) ?? listener, options);
  };
}
