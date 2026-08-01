import { describe, expect, it } from "vitest";

import { foldForkFamilies } from "./threadForkFamily.ts";

interface Row {
  readonly id: string;
  readonly forkFamilyId?: string | null;
}

const rows = (...entries: ReadonlyArray<Row>) => entries;

describe("foldForkFamilies", () => {
  it("collapses a multi-member family into its first row", () => {
    const folded = foldForkFamilies(
      rows({ id: "b", forkFamilyId: "fam" }, { id: "solo" }, { id: "a", forkFamilyId: "fam" }),
      new Set(),
    );
    expect(folded.rows.map((row) => row.id)).toEqual(["b", "solo"]);
    expect(folded.metaByThreadId.get("b")).toEqual({
      familyId: "fam",
      memberCount: 2,
      role: "head",
      expanded: false,
    });
    expect(folded.metaByThreadId.has("solo")).toBe(false);
  });

  it("expands branches in place directly under the head", () => {
    const folded = foldForkFamilies(
      rows(
        { id: "b", forkFamilyId: "fam" },
        { id: "solo" },
        { id: "a", forkFamilyId: "fam" },
        { id: "c", forkFamilyId: "fam" },
      ),
      new Set(["fam"]),
    );
    expect(folded.rows.map((row) => row.id)).toEqual(["b", "a", "c", "solo"]);
    expect(folded.metaByThreadId.get("b")?.role).toBe("head");
    expect(folded.metaByThreadId.get("b")?.expanded).toBe(true);
    expect(folded.metaByThreadId.get("a")?.role).toBe("branch");
    expect(folded.metaByThreadId.get("c")?.role).toBe("branch");
  });

  it("renders single-member families and unstamped threads as plain rows", () => {
    const folded = foldForkFamilies(
      rows({ id: "only", forkFamilyId: "fam" }, { id: "none", forkFamilyId: null }),
      new Set(["fam"]),
    );
    expect(folded.rows.map((row) => row.id)).toEqual(["only", "none"]);
    expect(folded.metaByThreadId.size).toBe(0);
  });

  it("keeps independent families independent", () => {
    const folded = foldForkFamilies(
      rows(
        { id: "a1", forkFamilyId: "a" },
        { id: "b1", forkFamilyId: "b" },
        { id: "a2", forkFamilyId: "a" },
        { id: "b2", forkFamilyId: "b" },
      ),
      new Set(["b"]),
    );
    expect(folded.rows.map((row) => row.id)).toEqual(["a1", "b1", "b2"]);
    expect(folded.metaByThreadId.get("a1")?.expanded).toBe(false);
    expect(folded.metaByThreadId.get("b1")?.expanded).toBe(true);
  });
});
