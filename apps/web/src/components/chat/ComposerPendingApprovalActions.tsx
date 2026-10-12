import {
  type ProviderApprovalDecision,
  type ProviderApprovalOption,
  type RuntimeRequestId,
} from "@t3tools/contracts";
import {
  approveForSessionLabel,
  approveWithModeLabel,
} from "@t3tools/client-runtime/state/thread-requests";
import { memo, useState } from "react";
import { EllipsisIcon, TriangleAlertIcon } from "lucide-react";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Menu, MenuItem, MenuPopup, MenuTrigger } from "../ui/menu";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import { composerFloatingLayerProps } from "./composerEventScope";

interface ComposerPendingApprovalActionsProps {
  requestId: RuntimeRequestId;
  isResponding: boolean;
  canRespond: boolean;
  options?: ReadonlyArray<ProviderApprovalOption> | undefined;
  /** The request takes the person's own words with a decline. */
  declineNote?: boolean | undefined;
  /** A permission mode an approval of the request can also switch the session to. */
  approveWithMode?: string | undefined;
  /** What approving for the session grants beyond this request. */
  sessionGrant?: string | undefined;
  onRespondToApproval: (
    requestId: RuntimeRequestId,
    decision: ProviderApprovalDecision,
    note?: string,
    mode?: string,
  ) => Promise<unknown>;
}

const DEFAULT_APPROVAL_OPTIONS = [
  { decision: "cancel", label: "Cancel" },
  { decision: "decline", label: "Decline" },
  { decision: "acceptForSession", label: "Always allow this session" },
  { decision: "accept", label: "Approve" },
] satisfies ReadonlyArray<ProviderApprovalOption>;

export const ComposerPendingApprovalActions = memo(function ComposerPendingApprovalActions({
  requestId,
  isResponding,
  canRespond,
  options = DEFAULT_APPROVAL_OPTIONS,
  declineNote = false,
  approveWithMode,
  sessionGrant,
  onRespondToApproval,
}: ComposerPendingApprovalActionsProps) {
  // Mounted per request (keyed by its id), so a note never outlives its request.
  const [note, setNote] = useState<string | null>(null);
  const primaryOptions = options.filter(
    (option) => option.decision === "decline" || option.decision === "accept",
  );
  const moreOptions = options.filter(
    (option) => option.decision !== "decline" && option.decision !== "accept",
  );

  if (note !== null) {
    const declineWithNote = () => void onRespondToApproval(requestId, "decline", note);
    return (
      <>
        <span className="w-64 max-w-full">
          <Input
            autoFocus
            size="sm"
            aria-label="What to do instead"
            placeholder="What to do instead"
            value={note}
            disabled={isResponding || !canRespond}
            data-approval-decline-note="true"
            onChange={(event) => setNote(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Escape") {
                event.preventDefault();
                event.stopPropagation();
                setNote(null);
              } else if (event.key === "Enter" && !event.nativeEvent.isComposing) {
                event.preventDefault();
                event.stopPropagation();
                declineWithNote();
              }
            }}
          />
        </span>
        <Button
          size="xs"
          variant="outline"
          disabled={isResponding || !canRespond}
          onClick={declineWithNote}
        >
          Decline
        </Button>
        <Button size="xs" variant="ghost" disabled={isResponding} onClick={() => setNote(null)}>
          Back
        </Button>
      </>
    );
  }

  return (
    <>
      {primaryOptions.map((option) => {
        const button = (
          <Button
            key={option.decision}
            size="xs"
            variant={option.decision === "accept" ? "default" : "outline"}
            disabled={isResponding || !canRespond}
            aria-description={option.warning}
            onClick={() => void onRespondToApproval(requestId, option.decision)}
          >
            {option.warning ? <TriangleAlertIcon className="size-3 shrink-0" /> : null}
            <span className="max-w-40 truncate">{option.label}</span>
          </Button>
        );
        return option.warning ? (
          <Tooltip key={option.decision}>
            <TooltipTrigger render={button} />
            <TooltipPopup side="top">{option.warning}</TooltipPopup>
          </Tooltip>
        ) : (
          button
        );
      })}
      {moreOptions.length > 0 || declineNote || approveWithMode !== undefined ? (
        <Menu>
          <MenuTrigger
            disabled={isResponding}
            render={<Button size="icon-xs" variant="outline" aria-label="More approval options" />}
          >
            <EllipsisIcon />
          </MenuTrigger>
          <MenuPopup {...composerFloatingLayerProps} side="top" align="end">
            {approveWithMode !== undefined ? (
              <MenuItem
                disabled={isResponding || !canRespond}
                onClick={() =>
                  void onRespondToApproval(requestId, "accept", undefined, approveWithMode)
                }
                variant="ghost"
                className="mb-1 last:mb-0"
              >
                <span className="min-w-0 whitespace-normal wrap-break-word">
                  {approveWithModeLabel(approveWithMode)}
                </span>
              </MenuItem>
            ) : null}
            {declineNote ? (
              <MenuItem
                disabled={isResponding || !canRespond}
                onClick={() => setNote("")}
                variant="ghost"
                className="mb-1 last:mb-0"
              >
                <span className="min-w-0 whitespace-normal wrap-break-word">
                  Decline and say what to do instead
                </span>
              </MenuItem>
            ) : null}
            {moreOptions.map((option) => {
              const item = (
                <MenuItem
                  key={option.decision}
                  disabled={isResponding}
                  aria-description={option.warning}
                  onClick={() => void onRespondToApproval(requestId, option.decision)}
                  variant="ghost"
                  className="mb-1 last:mb-0"
                >
                  {option.warning ? <TriangleAlertIcon className="size-3 text-warning" /> : null}
                  <span className="min-w-0 max-w-xl whitespace-normal wrap-break-word">
                    {option.decision === "acceptForSession"
                      ? approveForSessionLabel(option.label, sessionGrant)
                      : option.label}
                  </span>
                </MenuItem>
              );
              return option.warning ? (
                <Tooltip key={option.decision}>
                  <TooltipTrigger render={item} />
                  <TooltipPopup side="top">{option.warning}</TooltipPopup>
                </Tooltip>
              ) : (
                item
              );
            })}
          </MenuPopup>
        </Menu>
      ) : null}
    </>
  );
});
