import type {
  ModelSelection,
  ProviderInteractionMode,
  RuntimeMode,
  ServerProvider,
} from "@t3tools/contracts";

/**
 * Said when the person picks a permission mode on a thread whose session
 * keeps its own. Only Claude Code sessions do today, and the wording is theirs.
 */
export const SESSION_PERMISSION_MODE_NOTICE =
  "Permission mode belongs to the Claude Code session: shift+tab in its terminal. /plan works from here.";

/** Said when the person changes a model option the session keeps for itself. */
export const SESSION_MODEL_OPTIONS_NOTICE =
  "Model options belong to the Claude Code session: /effort and /model work from here.";

/**
 * Whether a thread's model and modes belong to its native session rather
 * than to the composer. A thread that has not started has no session yet, so
 * what the composer holds is still how its session will be created.
 */
export function threadSettingsBelongToSession(input: {
  readonly hasStartedThread: boolean;
  readonly provider: Pick<ServerProvider, "sessionOwnsThreadSettings"> | null | undefined;
}): boolean {
  return input.hasStartedThread && input.provider?.sessionOwnsThreadSettings === true;
}

/**
 * The session's own command for a model picked in T3, or null when the pick is
 * the model the session already runs. The thread shows the new model once the
 * session reports it.
 */
export function sessionModelCommand(input: {
  readonly current: Pick<ModelSelection, "instanceId" | "model">;
  readonly picked: Pick<ModelSelection, "instanceId" | "model">;
}): string | null {
  const model = input.picked.model.trim();
  if (model.length === 0) return null;
  if (input.picked.instanceId === input.current.instanceId && model === input.current.model) {
    return null;
  }
  return `/model ${model}`;
}

/**
 * What a started thread shows and sends as its model and modes: the thread's
 * own when its session keeps them, else the composer's saved copy over the
 * thread's. A copy that names another provider is a handoff the person asked
 * for, and stays the composer's.
 */
export function resolveStartedThreadSettings(input: {
  readonly thread: {
    readonly modelSelection: ModelSelection;
    readonly runtimeMode: RuntimeMode;
    readonly interactionMode: ProviderInteractionMode;
  };
  readonly draft:
    | {
        readonly modelSelection?: ModelSelection | null | undefined;
        readonly runtimeMode?: RuntimeMode | null | undefined;
        readonly interactionMode?: ProviderInteractionMode | null | undefined;
      }
    | null
    | undefined;
  readonly providers: ReadonlyArray<
    Pick<ServerProvider, "instanceId" | "sessionOwnsThreadSettings">
  >;
}): {
  readonly belongToSession: boolean;
  readonly modelSelection: ModelSelection;
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
} {
  const { thread, draft } = input;
  const draftInstanceId = draft?.modelSelection?.instanceId;
  const isHandoff =
    draftInstanceId !== undefined && draftInstanceId !== thread.modelSelection.instanceId;
  const belongToSession =
    !isHandoff &&
    threadSettingsBelongToSession({
      hasStartedThread: true,
      provider: input.providers.find(
        (provider) => provider.instanceId === thread.modelSelection.instanceId,
      ),
    });
  if (belongToSession) {
    return {
      belongToSession,
      modelSelection: thread.modelSelection,
      runtimeMode: thread.runtimeMode,
      interactionMode: thread.interactionMode,
    };
  }
  return {
    belongToSession,
    modelSelection: draft?.modelSelection ?? thread.modelSelection,
    runtimeMode: draft?.runtimeMode ?? thread.runtimeMode,
    interactionMode: draft?.interactionMode ?? thread.interactionMode,
  };
}
