import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { runInNewContext } from "node:vm";
import ts from "typescript";

/** Load the actual route source with explicit I/O replacements. No production configuration is loaded. */
export async function isolatedModule<T>(url: URL, replacements: Record<string, unknown>): Promise<T> {
  const source = await readFile(url, "utf8"), module = { exports: {} }, require = createRequire(url);
  const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, esModuleInterop: true } });
  runInNewContext(compiled.outputText, { module, exports: module.exports, Buffer, URL, Date, Intl, setTimeout, clearTimeout,
    fetch: () => { throw new Error("External network is forbidden in isolated SM tests"); },
    require: (name: string) => {
      if (Object.hasOwn(replacements, name)) return replacements[name];
      if (name.startsWith(".")) throw new Error(`An isolated replacement is required for ${name}`);
      return require(name);
    },
  }, { filename: url.pathname });
  return module.exports as T;
}
