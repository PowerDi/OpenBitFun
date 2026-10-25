import type { AppearanceSurfaceDescriptor } from '@/infrastructure/appearance';

export const toolApprovalBarAppearanceDescriptor: AppearanceSurfaceDescriptor = {
  id: 'tool-approval-bar',
  parts: [
    { id: 'root' },
    { id: 'main' },
    { id: 'message' },
    { id: 'planReview' },
    { id: 'plan' },
    { id: 'actions' },
  ],
};
