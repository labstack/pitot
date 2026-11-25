// Lists every .html file under a directory, for the checks that read `dist/`.
import { readdir } from "node:fs/promises";
import { join } from "node:path";

export async function htmlFiles(dir) {
  const found = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) found.push(...(await htmlFiles(path)));
    else if (entry.name.endsWith(".html")) found.push(path);
  }
  return found;
}
