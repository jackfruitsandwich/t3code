import { RequestActionButton } from "./RequestActionButton";
import type {
  ProviderApprovalDecision,
  ProviderApprovalOption,
  RuntimeRequestId,
} from "@t3tools/contracts";
import { approveWithModeLabel } from "@t3tools/client-runtime/state/thread-requests";
import { useState } from "react";
import { Platform, ScrollView, View } from "react-native";

import { AppText as Text, AppTextInput as TextInput } from "../../components/AppText";
import type { PendingApproval } from "../../lib/threadActivity";

export interface PendingApprovalCardProps {
  readonly approval: PendingApproval;
  readonly respondingApprovalId: RuntimeRequestId | null;
  readonly onRespond: (
    requestId: RuntimeRequestId,
    decision: ProviderApprovalDecision,
    note?: string,
    mode?: string,
  ) => Promise<unknown>;
  readonly onInputFocusChange?: (focused: boolean) => void;
}

const DEFAULT_APPROVAL_OPTIONS: ReadonlyArray<ProviderApprovalOption> = [
  { decision: "accept", label: "Allow once" },
  { decision: "acceptForSession", label: "Allow session" },
  { decision: "decline", label: "Decline" },
];

export function PendingApprovalCard(props: PendingApprovalCardProps) {
  const options: ReadonlyArray<ProviderApprovalOption> =
    props.approval.options ?? DEFAULT_APPROVAL_OPTIONS;
  const warning = options.find((option) => option.warning)?.warning;
  // Opaque for the same reason as PendingUserInputCard: nothing blurs the feed
  // behind this card, so a translucent surface bleeds messages through it.
  const canRespond = props.approval.responseCapability === "live";
  const disabled = !canRespond || props.respondingApprovalId === props.approval.requestId;
  // A request can carry what it would change under its first line (a file's
  // new lines, an edit's removed and added ones). Those keep their columns:
  // monospace, unwrapped, in a block that scrolls instead of growing the card.
  const detail = props.approval.detail ?? "";
  const firstBreak = detail.indexOf("\n");
  const summary = firstBreak === -1 ? detail : detail.slice(0, firstBreak);
  const change = firstBreak === -1 ? "" : detail.slice(firstBreak + 1);
  // Mounted per request (keyed by its id), so a note never outlives its request.
  const [note, setNote] = useState("");
  const takesNote = canRespond && props.approval.declineNote === true;
  const withMode = props.approval.approveWithMode;
  return (
    <View className="gap-2.5 rounded-[20px] border border-border bg-card-alt p-4">
      <Text className="font-t3-bold text-2xs uppercase tracking-[1.1px] text-foreground-secondary">
        Approval needed
      </Text>
      <Text className="font-t3-bold text-lg text-foreground">
        {props.approval.appName ?? props.approval.requestKind}
      </Text>
      {summary ? (
        <Text className="font-sans text-sm leading-normal text-foreground-secondary">
          {summary}
        </Text>
      ) : null}
      {change ? (
        <ScrollView
          nestedScrollEnabled
          className="max-h-40 rounded-lg border border-border bg-subtle"
        >
          <ScrollView
            horizontal
            bounces={false}
            nestedScrollEnabled={Platform.OS === "android"}
            showsHorizontalScrollIndicator={false}
            contentContainerClassName="px-3 py-2"
          >
            <Text selectable className="font-mono text-2xs leading-normal text-foreground">
              {change}
            </Text>
          </ScrollView>
        </ScrollView>
      ) : null}
      {!canRespond ? (
        <Text className="font-sans text-sm leading-5 text-adaptive-neutral-600-400">
          The provider process for this request is no longer available. Interrupt or restart the run
          to continue.
        </Text>
      ) : null}
      {warning ? (
        <Text className="font-sans text-xs leading-normal text-warning-foreground">{warning}</Text>
      ) : null}
      {takesNote ? (
        <TextInput
          accessibilityLabel="What to do instead"
          placeholder="To decline with your own words, say what to do instead"
          value={note}
          onChangeText={setNote}
          editable={!disabled}
          multiline
          onFocus={() => props.onInputFocusChange?.(true)}
          onBlur={() => props.onInputFocusChange?.(false)}
          className="min-h-[44px] rounded-2xl border border-input-border bg-input px-3.5 py-2.5 font-sans text-base text-foreground"
        />
      ) : null}
      <View className="flex-row flex-wrap gap-2.5">
        {options.map((option) => (
          <RequestActionButton
            key={option.decision}
            label={option.label}
            tone={
              option.decision === "accept"
                ? "primary"
                : option.decision === "decline"
                  ? "danger"
                  : "secondary"
            }
            disabled={disabled}
            onPress={() =>
              void props.onRespond(
                props.approval.requestId,
                option.decision,
                takesNote && option.decision === "decline" && note.trim().length > 0
                  ? note
                  : undefined,
              )
            }
          />
        ))}
        {canRespond && withMode !== undefined ? (
          <RequestActionButton
            label={approveWithModeLabel(withMode)}
            tone="secondary"
            disabled={disabled}
            onPress={() =>
              void props.onRespond(props.approval.requestId, "accept", undefined, withMode)
            }
          />
        ) : null}
      </View>
    </View>
  );
}
