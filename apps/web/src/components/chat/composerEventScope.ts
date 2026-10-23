import { createContext, useContext } from "react";

import { useComposerHandleContext } from "../../composerHandleContext";

/** The provider look of the composer that owns a floating layer; portaled menus carry it. */
export type ComposerGlass = "claude";
export const ComposerGlassContext = createContext<ComposerGlass | undefined>(undefined);

export function useComposerGlass() {
  return useContext(ComposerGlassContext);
}

const COMPOSER_FLOATING_LAYER_SELECTOR = [
  '[data-composer-drawer-layer="true"]',
  '[data-chat-composer-floating-layer="true"]',
].join(",");

export const composerFloatingLayerProps = {
  "data-chat-composer-floating-layer": "true",
} as const;

export function useComposerMenuProps() {
  const composerRef = useComposerHandleContext();
  const composerGlass = useComposerGlass();

  return {
    ...composerFloatingLayerProps,
    "data-composer-glass": composerGlass,
    finalFocus: composerRef
      ? () => {
          const activeElement = document.activeElement;
          if (activeElement !== document.body && !isInsideComposerFloatingLayer(activeElement)) {
            return false;
          }
          composerRef.current?.focusAtEnd();
          return false;
        }
      : undefined,
  };
}

export function isInsideComposerFloatingLayer(target: EventTarget | null): boolean {
  return target instanceof Element && target.closest(COMPOSER_FLOATING_LAYER_SELECTOR) !== null;
}

// Banners, the approval row, and the tasks badge dock above the surface. A
// pointer or focus landing on one of them acts on that control and must not
// expand a resting or collapsed composer.
export function isInsideCollapsedComposerControls(target: EventTarget | null): boolean {
  return (
    target instanceof Element &&
    target.closest('[data-chat-composer-collapsed-controls="true"]') !== null
  );
}

export function isInsideRestingComposerControlScope(target: EventTarget | null): boolean {
  return (
    target instanceof Element &&
    (target.closest('[data-chat-composer-resting-controls="true"]') !== null ||
      target.closest('[data-chat-composer-resting-images="true"]') !== null ||
      target.closest("[data-composer-context-control]") !== null ||
      isInsideComposerFloatingLayer(target))
  );
}
