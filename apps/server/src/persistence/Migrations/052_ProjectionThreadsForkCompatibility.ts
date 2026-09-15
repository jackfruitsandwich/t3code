import * as Effect from "effect/Effect";

import migrateTitleRegeneration from "./035_ProjectionThreadTitleRegeneration.ts";
import migrateForkFamily from "./035_ProjectionThreadsForkFamily.ts";

// The installed personal fork used migration 35 for fork families before
// upstream assigned 35 to title regeneration. Migrations run by numeric ID,
// so those installs skip upstream 35. Repair both schema branches without
// rewriting migration history, conversation events, or existing family IDs.
export default Effect.gen(function* () {
  yield* migrateTitleRegeneration;
  yield* migrateForkFamily;
});
