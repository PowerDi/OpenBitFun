import type { AppearanceSurfaceDescriptor } from '@/infrastructure/appearance';

export const nativeWakeNoticeAppearanceDescriptor: AppearanceSurfaceDescriptor = {
  id: 'native-wake-notice',
  componentAttribute: 'data-openbitfun-product-component',
  parts: [{ id: 'root' }],
};
