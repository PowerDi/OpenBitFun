import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const repositoryRoot = fileURLToPath(new URL('../../../../../', import.meta.url));
const stylesheet = readFileSync(
  resolve(repositoryRoot, 'design-system/packages/ui/src/styles/scrollbars.css'),
  'utf8',
).replace(/\r\n?/g, '\n');

describe('shared native scrollbar presentation', () => {
  it('keeps every non-thumb WebKit surface transparent', () => {
    expect(stylesheet).toMatch(/::-webkit-scrollbar\s*\{[^}]*background:\s*transparent;/s);

    for (const part of ['track', 'track-piece', 'corner', 'button']) {
      expect(stylesheet).toContain(`::-webkit-scrollbar-${part}`);
    }
    expect(stylesheet).toMatch(/::-webkit-resizer\s*\{\s*background:\s*transparent;/s);
    expect(stylesheet).toMatch(
      /::-webkit-scrollbar-thumb\s*\{[^}]*background:\s*var\(--openbitfun-color-scrollbar-thumb\);/s,
    );
    expect(stylesheet).toMatch(
      /::-webkit-scrollbar-thumb:hover,[^{}]+::-webkit-scrollbar-thumb:active\s*\{[^}]*background:\s*var\(--openbitfun-color-scrollbar-thumb-hover\);/s,
    );
  });

  it('preserves automatic track geometry and only releases explicitly opted-in hover tracks', () => {
    const hoverStart = stylesheet.indexOf('/* Opt-in hover viewports');
    const hiddenStart = stylesheet.indexOf('/* Explicit hidden viewports');
    const interactionBlocks = Array.from(
      stylesheet
        .slice(0, hoverStart)
        .matchAll(/@media \(hover: hover\) and \(pointer: fine\) and \(forced-colors: none\)\s*\{([\s\S]*?)\n    \}/g),
      match => match[1],
    );
    const hoverRules = stylesheet.slice(hoverStart, hiddenStart);
    const positiveRevealSelector =
      ':where(:hover, :focus-visible, [data-openbitfun-scrollbar-visibility="always"], :root:not([data-openbitfun-focus-visible="false"]) :focus-within, :root:not([data-openbitfun-focus-visible="false"]):focus-within)';

    expect(interactionBlocks).toHaveLength(2);
    expect(interactionBlocks[0]).toContain('background: transparent;');
    expect(interactionBlocks[0]).toContain(positiveRevealSelector);
    expect(interactionBlocks[0]).toContain('background: var(--openbitfun-color-scrollbar-thumb);');
    expect(interactionBlocks[1]).toContain('scrollbar-color: transparent transparent;');
    expect(interactionBlocks[1]).toContain(positiveRevealSelector);
    expect(interactionBlocks[1]).toContain('scrollbar-color: var(--openbitfun-color-scrollbar-thumb) transparent;');
    for (const block of interactionBlocks) {
      expect(block).not.toMatch(/(?:overflow|width|height|display|scrollbar-gutter)\s*:/);
    }

    expect(hoverRules).toContain('@media (hover: hover) and (pointer: fine) and (forced-colors: none)');
    expect(hoverRules).toMatch(
      /\[data-openbitfun-scrollbar-visibility="hover"\]:not\(:hover, :focus-visible\):where\(:not\(:focus-within\), :root\[data-openbitfun-focus-visible="false"\]\[data-openbitfun-scrollbar-visibility="hover"\], :root\[data-openbitfun-focus-visible="false"\] \[data-openbitfun-scrollbar-visibility="hover"\]\)\s*\{\s*scrollbar-width:\s*none;/,
    );
    expect(hoverRules).toMatch(
      /\[data-openbitfun-scrollbar-visibility="hover"\]:not\(:hover, :focus-visible\):where\(:not\(:focus-within\), :root\[data-openbitfun-focus-visible="false"\]\[data-openbitfun-scrollbar-visibility="hover"\], :root\[data-openbitfun-focus-visible="false"\] \[data-openbitfun-scrollbar-visibility="hover"\]\)::-webkit-scrollbar\s*\{\s*display:\s*none;/,
    );
    const rulesOnly = stylesheet.replace(/\/\*[\s\S]*?\*\//g, '');
    expect(hoverRules).not.toMatch(/(?:overflow(?:-[xy])?|scrollbar-gutter)\s*:/);
    expect(rulesOnly).not.toMatch(/\[data-openbitfun-focus-visible="false"\]\s*\*/);
    expect(rulesOnly).not.toMatch(/\.is-scrolling|:has\(|!important/);
  });

  it('guards standard width and color together so Safari 18 keeps the WebKit path', () => {
    expect(stylesheet).toMatch(
      /@supports not \(scrollbar-color: transparent transparent\)\s*\{[\s\S]*::-webkit-scrollbar\s*\{/,
    );
    expect(stylesheet).toMatch(
      /@supports \(scrollbar-color: transparent transparent\)\s*\{[^{}]+\{\s*scrollbar-width:\s*thin;\s*scrollbar-color:\s*var\(--openbitfun-color-scrollbar-thumb\) transparent;/,
    );
    expect(stylesheet.match(/scrollbar-width:\s*thin/g)).toHaveLength(1);
    expect(stylesheet).toMatch(
      /@media \(forced-colors: active\)\s*\{[\s\S]*?background:\s*CanvasText;/,
    );
    expect(stylesheet).toMatch(
      /@media \(forced-colors: active\)\s*\{[^{}]+\{\s*scrollbar-color:\s*auto;/,
    );
  });

  it('keeps native scrollbar paint in one owner across product and component styles', () => {
    const roots = [
      'src/web-ui/src',
      'design-system/packages/ui/src/components',
      'design-system/packages/ui/src/flow-chat',
    ];
    const overrides: string[] = [];

    for (const root of roots) {
      const directory = resolve(repositoryRoot, root);
      for (const relative of readdirSync(directory, { recursive: true })) {
        if (typeof relative !== 'string' || !/\.(css|scss)$/.test(relative)) continue;
        const content = readFileSync(resolve(directory, relative), 'utf8');
        if (/scrollbar-color\s*:|scrollbar-width\s*:\s*thin|::-webkit-scrollbar-thumb\b/.test(content)) {
          overrides.push(`${root}/${relative}`);
        }
      }
    }

    expect(overrides).toEqual([]);
  });
});
