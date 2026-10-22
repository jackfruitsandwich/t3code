import { sessionDraftNotice } from "@t3tools/client-runtime/state/sessionDraft";
import { Pressable, View } from "react-native";

import { AppText as Text } from "../../components/AppText";
import { SymbolView } from "../../components/AppSymbol";

/**
 * Shown while the composer and the terminal's prompt box hold different
 * drafts: neither is overwritten until one is taken or this is dismissed.
 */
export function ComposerSessionDraftBanner(props: {
  readonly draft: string;
  readonly onTake: () => void;
  readonly onDismiss: () => void;
}) {
  const notice = sessionDraftNotice(props.draft);
  return (
    <View className="flex-row items-center gap-2 px-4 pb-2">
      <SymbolView name="terminal" size={12} tintColorClassName="accent-foreground-muted" />
      <View className="min-w-0 flex-1">
        <Text className="text-xs text-foreground-muted" numberOfLines={1}>
          {notice.title}
        </Text>
        {notice.preview === null ? null : (
          <Text className="text-xs text-foreground" numberOfLines={2}>
            {notice.preview}
          </Text>
        )}
      </View>
      <Pressable
        accessibilityRole="button"
        accessibilityLabel={notice.takeLabel}
        onPress={props.onTake}
        hitSlop={8}
        className="min-h-8 justify-center px-1 active:opacity-70"
      >
        <Text className="font-t3-medium text-xs text-primary">{notice.takeLabel}</Text>
      </Pressable>
      <Pressable
        accessibilityRole="button"
        accessibilityLabel="Keep my draft"
        onPress={props.onDismiss}
        hitSlop={8}
        className="min-h-8 justify-center px-1 active:opacity-60"
      >
        <SymbolView name="xmark" size={12} tintColorClassName="accent-icon-muted" type="monochrome" />
      </Pressable>
    </View>
  );
}
