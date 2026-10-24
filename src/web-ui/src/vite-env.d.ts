/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Fork release builds only: upstream `owner/repo` whose releases are announced, never installed. */
  readonly VITE_OPENBITFUN_UPSTREAM_WATCH_REPO?: string;
}

declare module 'virtual:openbitfun-canvas-runtime-bundle' {
  const bundle: {
    js: string;
    css: string;
  };
  export const openbitfunCanvasRuntimeBundle: typeof bundle;
  export default bundle;
}
