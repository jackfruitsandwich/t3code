import { describe, expect, it } from "vite-plus/test";

import { foldForkFamilies, paginateForkFamilies } from "./threadForkFamily.ts";

interface Row {
  readonly id: string;
  readonly environmentId?: string;
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
  it("keeps identical session and family IDs on different machines separate", () => {
    const input = rows(
      { id: "head", environmentId: "mac", forkFamilyId: "family" },
      { id: "head", environmentId: "box", forkFamilyId: "family" },
      { id: "branch", environmentId: "mac", forkFamilyId: "family" },
      { id: "branch", environmentId: "box", forkFamilyId: "family" },
    );
    const folded = foldForkFamilies(input, new Set(["mac:family"]));
    expect(folded.rows.map((row) => `${row.environmentId}:${row.id}`)).toEqual([
      "mac:head",
      "mac:branch",
      "box:head",
    ]);
    expect(folded.metaByThreadId.get("mac:head")?.memberCount).toBe(2);
    expect(folded.metaByThreadId.get("box:head")?.expanded).toBe(false);
  });

  it("keeps the selected branch reachable when its family was collapsed", () => {
    const input = rows({ id: "a", forkFamilyId: "family" }, { id: "b", forkFamilyId: "family" });
    expect(foldForkFamilies(input, new Set(), "b").rows).toEqual(input);
    expect(foldForkFamilies(input, new Set(), "a").rows).toEqual([input[0]]);
  });
});

describe("paginateForkFamilies", () => {
  const threads = rows(
    { id: "head", forkFamilyId: "family" },
    { id: "solo" },
    { id: "branch-1", forkFamilyId: "family" },
    { id: "branch-2", forkFamilyId: "family" },
    { id: "next-head", forkFamilyId: "next" },
    { id: "next-branch", forkFamilyId: "next" },
  );
  it("reveals every branch at a page boundary and leaves other conversations paged", () => {
    const folded = foldForkFamilies(threads, new Set(["family", "next"]));
    expect(paginateForkFamilies(folded, 1).map((thread) => thread.id)).toEqual([
      "head",
      "branch-1",
      "branch-2",
    ]);
    expect(paginateForkFamilies(folded, 2).map((thread) => thread.id)).toEqual([
      "head",
      "branch-1",
      "branch-2",
      "solo",
    ]);
  });
  it("keeps a selected deep branch with its parent even below the visible page", () => {
    const folded = foldForkFamilies(threads, new Set(), "next-branch");
    expect(paginateForkFamilies(folded, 1, "next-branch").map((thread) => thread.id)).toEqual([
      "head",
      "next-head",
      "next-branch",
    ]);
  });
});
