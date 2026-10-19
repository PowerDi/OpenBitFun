import { Button, OverflowText, Textarea } from '@openbitfun/ui';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { MarkdownRenderer } from '@/infrastructure/markdown';
import { AcpPermissionActions } from '../tool-cards/AcpPermissionActions';
import { hasAcpPermissionOptions } from '../tool-cards/AcpPermissionActions.utils';
import type { FlowToolItem, ToolRejectOptions } from '../types/flow-chat';
import './ToolApprovalBar.scss';

/** Option ids the Claude native driver offers for an `ExitPlanMode` review. */
const APPROVE_PLAN_OPTION = 'approve_plan';
const KEEP_PLANNING_OPTION = 'keep_planning';

interface ToolApprovalBarProps {
  toolItem: FlowToolItem;
  onConfirm?: (permissionOptionId?: string, approve?: boolean) => void;
  onReject?: (options?: ToolRejectOptions) => void;
}

function planReviewText(toolItem: FlowToolItem): string | null {
  const options = toolItem.acpPermission?.options ?? [];
  if (!options.some(option => option.optionId === APPROVE_PLAN_OPTION)) return null;
  const input = (toolItem.acpPermission?.toolCall?.rawInput ?? toolItem.toolCall?.input) as
    | { plan?: unknown }
    | undefined;
  return typeof input?.plan === 'string' ? input.plan : '';
}

const PlanReview: React.FC<{
  plan: string;
  onConfirm?: ToolApprovalBarProps['onConfirm'];
  onReject?: ToolApprovalBarProps['onReject'];
}> = ({ plan, onConfirm, onReject }) => {
  const { t } = useTranslation('flow-chat');
  const [feedback, setFeedback] = useState('');
  return (
    <div data-openbitfun-component="tool-approval-bar" data-openbitfun-part="planReview" className="tool-approval-bar__plan-review">
      <OverflowText data-openbitfun-component="tool-approval-bar" data-openbitfun-part="message" className="tool-approval-bar__message">
        {t('toolCards.planReview.title')}
      </OverflowText>
      {plan.trim() ? (
        <div data-openbitfun-component="tool-approval-bar" data-openbitfun-part="plan" className="tool-approval-bar__plan">
          <MarkdownRenderer content={plan} />
        </div>
      ) : (
        <OverflowText className="tool-approval-bar__message">{t('toolCards.planReview.missingPlan')}</OverflowText>
      )}
      <Textarea
        className="tool-approval-bar__plan-feedback"
        value={feedback}
        onChange={(event) => setFeedback(event.target.value)}
        placeholder={t('toolCards.planReview.feedbackPlaceholder')}
        aria-label={t('toolCards.planReview.feedbackPlaceholder')}
        rows={2}
      />
      <div data-openbitfun-component="tool-approval-bar" data-openbitfun-part="actions" className="tool-approval-bar__actions">
        <Button
          type="button"
          variant="fill"
          size="sm"
          data-openbitfun-decision="reject"
          onClick={() => onReject?.({ permissionOptionId: KEEP_PLANNING_OPTION, instruction: feedback })}
        >
          {t('toolCards.planReview.keepPlanning')}
        </Button>
        <Button
          type="button"
          variant="primary"
          size="sm"
          data-openbitfun-decision="allow"
          disabled={!plan.trim()}
          title={t('toolCards.planReview.approveHint')}
          onClick={() => onConfirm?.(APPROVE_PLAN_OPTION, true)}
        >
          {t('toolCards.planReview.approve')}
        </Button>
      </div>
    </div>
  );
};

export const ToolApprovalBar: React.FC<ToolApprovalBarProps> = ({
  toolItem,
  onConfirm,
  onReject,
}) => {
  const { t } = useTranslation('flow-chat');

  if (toolItem.status !== 'pending_confirmation' || !hasAcpPermissionOptions(toolItem)) {
    return null;
  }

  const plan = planReviewText(toolItem);
  if (plan !== null) {
    return (
      <div data-openbitfun-component="tool-approval-bar" data-openbitfun-part="root" className="tool-approval-bar" role="group" aria-label={t('toolCards.planReview.title')}>
        <PlanReview plan={plan} onConfirm={onConfirm} onReject={onReject} />
      </div>
    );
  }

  return (
    <div data-openbitfun-component="tool-approval-bar" data-openbitfun-part="root" className="tool-approval-bar" role="group" aria-label={t('toolCards.approval.ariaLabel')}>
      <div data-openbitfun-component="tool-approval-bar" data-openbitfun-part="main" className="tool-approval-bar__main">
        <OverflowText data-openbitfun-component="tool-approval-bar" data-openbitfun-part="message" className="tool-approval-bar__message">{t('toolCards.approval.waiting')}</OverflowText>
        <AcpPermissionActions
          toolItem={toolItem}
          presentation="text"
          className="tool-approval-bar__permission-actions"
          onConfirm={onConfirm}
          onReject={onReject}
        />
      </div>
    </div>
  );
};

export default ToolApprovalBar;
