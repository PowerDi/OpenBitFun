import type { Session } from '@/flow_chat/types/flow-chat';
import { resolveSessionRelationship } from '@/flow_chat/utils/sessionMetadata';
import { sessionOwningWorkspaceId } from '@/flow_chat/utils/sessionOrdering';

/** Fast parent-id resolver: avoid full relationship normalization for ordinary normal sessions. */
function resolveParentSessionId(session: Session): string | undefined {
  if (session.sessionKind === 'normal' && !session.parentSessionId && !session.btwOrigin) {
    return undefined;
  }
  return resolveSessionRelationship(session).parentSessionId;
}

/** Build the same workspace-owned hierarchy for both rows and their count. */
export function getWorkspaceSessionHierarchy(
  sessions: Iterable<Session>,
  workspaceIds: readonly string[],
): { topLevelSessions: Session[]; childrenByParent: Map<string, Session[]> } {
  const hasWorkspaceScope = workspaceIds.length > 0;
  const workspaceIdSet = hasWorkspaceScope ? new Set(workspaceIds) : null;
  const scoped: Session[] = [];
  const knownIds = new Set<string>();

  for (const session of sessions) {
    if (session.isTransient || session.sessionKind === 'subagent') continue;
    const ownerId = sessionOwningWorkspaceId(session);
    const inScope = hasWorkspaceScope
      ? (ownerId ? workspaceIdSet!.has(ownerId) : false)
      : !ownerId;
    if (!inScope) continue;
    scoped.push(session);
    knownIds.add(session.sessionId);
  }

  // Delegated native sessions nest under the delegating session.
  const parentIds = new Map<string, string>();
  for (let i = 0; i < scoped.length; i++) {
    const session = scoped[i];
    const parentId = resolveParentSessionId(session) ?? session.delegatedBy;
    if (parentId && knownIds.has(parentId)) parentIds.set(session.sessionId, parentId);
  }

  const topLevelSessions: Session[] = [];
  const childrenByParent = new Map<string, Session[]>();
  for (let i = 0; i < scoped.length; i++) {
    const session = scoped[i];
    // Nesting is one level: a child of a child lists under the top-level
    // ancestor so it is never hidden.
    const ancestorId = parentIds.size ? topLevelAncestor(session.sessionId, parentIds) : undefined;
    if (ancestorId) {
      const children = childrenByParent.get(ancestorId);
      if (children) {
        children.push(session);
      } else {
        childrenByParent.set(ancestorId, [session]);
      }
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
