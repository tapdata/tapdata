package com.tapdata.tm.autoinspect.constants;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/9/2 14:49 Create
 */
public class AutoInspectConstants {
    public static final String MODULE_NAME = "AutoInspect";
    public static final String AGAIN_MODULE_NAME = MODULE_NAME + "Again";
    public static final String NODE_TYPE = "auto_inspect";

    public static final String AUTO_INSPECT_PROGRESS_KEY = "autoInspectProgress";
    public static final String AUTO_INSPECT_PROGRESS_PATH = "attrs." + AUTO_INSPECT_PROGRESS_KEY;
    public static final String AUTO_INSPECT_RESULTS_COLLECTION_NAME = "AutoInspectResults";

    public static final long PROGRESS_UPDATE_INTERVAL = 10000L;
    public static final long CHECK_AGAIN_TIMEOUT = 60000L;
    public static final String CHECK_AGAIN_DEFAULT_SN = "-";
    public static final String CHECK_AGAIN_PROGRESS_KEY = "checkAgainProgress";
    public static final String CHECK_AGAIN_PROGRESS_PATH = "attrs." + CHECK_AGAIN_PROGRESS_KEY;

}
