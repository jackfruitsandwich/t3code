import { createSessionDraftEnvironmentAtoms } from "@t3tools/client-runtime/state/sessionDraft";

import { connectionAtomRuntime } from "../connection/runtime";

export const sessionDraftEnvironment = createSessionDraftEnvironmentAtoms(connectionAtomRuntime);
