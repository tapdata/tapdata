package com.tapdata.tm.sso.dto;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * Aggregate result of an SSO batch user import. For a validate (dry-run) call the
 * per-row {@link ImportRowResult#getStatus()} values describe what <em>would</em>
 * happen; for a confirmed call they describe what <em>did</em> happen. The count
 * fields are derived from {@link #getRows()}.
 */
@Data
public class ImportPreviewResult {

    /** True when this is a dry-run (no writes performed). */
    private boolean dryRun;

    /** Total data rows parsed from the file. */
    private int total;

    /** Rows that create a new user. */
    private int createCount;

    /** Rows that update an existing user (update mode). */
    private int updateCount;

    /** Rows left untouched (skip mode / no change). */
    private int skipCount;

    /** Rows rejected by validation. */
    private int failedCount;

    /** Per-row detail, in file order. */
    private List<ImportRowResult> rows = new ArrayList<>();

    /** Recompute the aggregate counts from {@link #rows}. */
    public void recomputeCounts() {
        int create = 0, update = 0, skip = 0, failed = 0;
        for (ImportRowResult r : rows) {
            if (r.getStatus() == null) {
                continue;
            }
            switch (r.getStatus()) {
                case CREATE: create++; break;
                case UPDATE: update++; break;
                case SKIP: skip++; break;
                case FAILED: failed++; break;
                default: break;
            }
        }
        this.total = rows.size();
        this.createCount = create;
        this.updateCount = update;
        this.skipCount = skip;
        this.failedCount = failed;
    }
}
