package com.tapdata.tm.sso.dto;

import lombok.Data;

import java.util.List;

/**
 * Per-row outcome of an SSO batch user import (validate dry-run or confirmed apply).
 * Row numbers are 1-based data rows (the header row is not counted).
 */
@Data
public class ImportRowResult {

    /** Outcome of a single import row. */
    public enum Status {
        /** New user would be / was created. */
        CREATE,
        /** Existing user would be / was updated (update mode only). */
        UPDATE,
        /** Existing user left untouched (skip mode, or nothing to change). */
        SKIP,
        /** Row rejected by validation (see {@link #getMessage()}). */
        FAILED
    }

    /** 1-based data-row index in the uploaded file. */
    private int row;

    /** Parsed email (may be blank when the row is malformed). */
    private String email;

    /** Parsed username. */
    private String username;

    /** Parsed display name. */
    private String displayName;

    /** Parsed role names. */
    private List<String> roleNames;

    /** Row outcome. */
    private Status status;

    /** Human-readable reason, primarily for {@link Status#FAILED} rows. */
    private String message;
}
