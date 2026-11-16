import rehypeRaw from 'rehype-raw';

function hastContainsRawNode(node: unknown): boolean {
  if (!node || typeof node !== 'object') {
    return false;
  }
  const record = node as { type?: string; children?: unknown[] };
  if (record.type === 'raw') {
    return true;
  }
  const children = record.children;
  if (Array.isArray(children)) {
    for (let i = 0; i < children.length; i += 1) {
      if (hastContainsRawNode(children[i])) {
        return true;
      }
    }
  }
  return false;
}

/**
 * Runs `rehype-raw` (`hast-util-raw` + `parse5` HTML5 fragment parser) only when
 * `remark-rehype` actually emitted at least one `raw` HTML node. Pure Markdown
 * trees already consist of standard HAST elements and avoid a full `parse5`
 * re-parse on every streaming frame.
 */
export function rehypeRawIfNeeded() {
  const rawTransform = rehypeRaw();
  return (tree: any, file: any) => (hastContainsRawNode(tree) ? rawTransform(tree, file) : tree);
}
