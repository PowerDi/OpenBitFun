import type { Session } from '@/flow_chat/types/flow-chat';
import { resolveSessionRelationship } from '@/flow_chat/utils/sessionMetadata';
import { sessionBelongsToWorkspaceNavRow } from '@/flow_chat/utils/sessionOrdering';

/** Build the same workspace-owned hierarchy for both rows and their count. */
export function getWorkspaceSessionHierarchy(
  sessions: Iterable<Session>,
  workspaceIds: readonly string[],
): { topLevelSessions: Session[]; childrenByParent: Map<string, Session[]> } {
  const scoped = Array.from(sessions).filter(session => {
    if (session.isTransient || session.sessionKind === 'subagent') return false;
    return workspaceIds.length
      ? workspaceIds.some(workspaceId => sessionBelongsToWorkspaceNavRow(session, workspaceId))
      : !session.workspaceId;
  });
  const knownIds = new Set(scoped.map(session => session.sessionId));
  // Delegated native sessions nest under the delegating session.
  const parentIds = new Map<string, string>();
  for (const session of scoped) {
    const parentId = resolveSessionRelationship(session).parentSessionId ?? session.delegatedBy;
    if (parentId && knownIds.has(parentId)) parentIds.set(session.sessionId, parentId);
  }
  const topLevelSessions: Session[] = [];
  const childrenByParent = new Map<string, Session[]>();
  for (const session of scoped) {
    // Nesting is one level: a child of a child lists under the top-level
    // ancestor so it is never hidden.
    const ancestorId = topLevelAncestor(session.sessionId, parentIds);
    if (ancestorId) {
      const children = childrenByParent.get(ancestorId) ?? [];
      children.push(session);
      childrenByParent.set(ancestorId, children);
    } else {
      topLevelSessions.push(session);
    }
  }
  return { topLevelSessions, childrenByParent };
}

/** Top-level ancestor of a child; `undefined` when top level or in a cycle. */
function topLevelAncestor(sessionId: string, parentIds: ReadonlyMap<string, string>): string | undefined {
  let current = parentIds.get(sessionId);
  if (!current) return undefined;
  const seen = new Set([sessionId]);
  for (let next = parentIds.get(current); next; next = parentIds.get(current)) {
    if (seen.has(current)) return undefined;
    seen.add(current);
    current = next;
  }
  return current;
}
