package com.tapdata.tm.task.constant;

public class SyncStatus {
    public static final String SYN_STATUS = "syncStatus";

    public static final String NORMAL = "";

    /** Full sync ongoing*/
    public static final String FULLING = "fulling";
    /** Full sync completed*/
    public static final String FULL_COMPLETED = "full_completed";
    public static final String FULL_FAILED = "full_failed";
    /** Incremental sync ongoing*/
    public static final String INCREMENTAL = "incremental";
    public static final String INCREMENTAL_FAILED = "incremental_failed";
}
