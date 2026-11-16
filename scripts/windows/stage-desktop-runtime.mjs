import { cpSync, readdirSync, readFileSync } from 'node:fs';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

export function stageDesktopRuntime(desktopDir, executable) {
  const generatedDir = join(desktopDir, 'gen');
  const configs = readdirSync(generatedDir).filter((name) =>
    /^tauri(?:\.[a-f0-9]{64})?\.generated\.conf\.json$/.test(name),
  );
  if (configs.length !== 1) {
    throw new Error(`Expected one generated Desktop bundle config in ${generatedDir}, found ${configs.length}`);
  }
  const config = JSON.parse(readFileSync(join(generatedDir, configs[0]), 'utf8'));
  const resources = config.bundle?.resources;
  if (!resources || Array.isArray(resources) || Object.keys(resources).length === 0) {
    throw new Error('The generated Desktop bundle config must declare a resource map');
  }

  const output = dirname(resolve(executable));
  for (const [source, destination] of Object.entries(resources)) {
    const target = resolve(output, destination);
    const inside = relative(output, target);
    if (!inside || inside === '..' || inside.startsWith(`..${sep}`) || isAbsolute(inside)) {
      throw new Error(`Desktop resource destination escapes the installer runtime: ${destination}`);
    }
    cpSync(resolve(desktopDir, source), target, { recursive: true });
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [desktopDir, executable] = process.argv.slice(2);
  if (!desktopDir || !executable) throw new Error('Usage: stage-desktop-runtime.mjs DESKTOP_DIR EXECUTABLE');
  stageDesktopRuntime(desktopDir, executable);
}
