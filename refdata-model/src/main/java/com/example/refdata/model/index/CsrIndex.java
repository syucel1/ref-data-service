package com.example.refdata.model.index;

/**
 * A one-to-many index in "compressed sparse row" form: {@code offsets[parent]..offsets[parent+1]} are the indices in
 * {@code children} of that parent's child positions. No per-parent objects; about 4 bytes per child.
 */
public final class CsrIndex {
    private final int[] offsets;
    private final int[] children;

    /**
     * @param parentOfChild for each child position, the parent position (or -1 if none)
     * @param parentCount   number of parents
     * @param order         child positions in the order they should appear within a parent (e.g. sorted by type, date); null = by position
     */
    public static CsrIndex build(int[] parentOfChild, int childCount, int parentCount, int[] order) {
        int[] offsets = new int[parentCount + 1];
        for (int c = 0; c < childCount; c++) { int p = parentOfChild[c]; if (p >= 0) offsets[p + 1]++; }
        for (int p = 0; p < parentCount; p++) offsets[p + 1] += offsets[p];
        int[] fill = offsets.clone();
        int[] children = new int[offsets[parentCount]];
        for (int i = 0; i < childCount; i++) {
            int c = order == null ? i : order[i];
            int p = parentOfChild[c];
            if (p >= 0) children[fill[p]++] = c;
        }
        return new CsrIndex(offsets, children);
    }

    private CsrIndex(int[] offsets, int[] children) { this.offsets = offsets; this.children = children; }

    public int count(int parentPos) { return parentPos < 0 ? 0 : offsets[parentPos + 1] - offsets[parentPos]; }

    /** The i-th child position of the parent (0 <= i < count). */
    public int child(int parentPos, int i) { return children[offsets[parentPos] + i]; }

    public int totalChildren() { return children.length; }
}
