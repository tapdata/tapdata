package io.tapdata.sybase.util;

/**
 * @author GavinXiao
 * @description ConfigPaths create by Gavin
 * @create 2023/8/8 15:22
 **/
public class ConfigPaths {
    public static final String SCHEMA_CONFIG_PATH = "/config/sybase2csv/csv/schemas.yaml";

    public static final String SYBASE_SRC_PATH = "/config/sybase2csv/src_sybasease.yaml";

    public static final String DST_LOCAL_STORAGE_PATH = "/config/sybase2csv/dst_localstorage.yaml";

    public static final String GENERAL_CONFIG_PATH = "/config/sybase2csv/general.yaml";

    public static final String EXT_CONFIG_PATH = "/config/sybase2csv/ext_sybasease.yaml";


    public static final String SYBASE_USE_CONFIG_DIR = "/config/sybase2csv";


    public static final String SYBASE_USE_TASK_CONFIG_BASE_DIR = "sybase_cdc_path";

    public static final String SYBASE_USE_TASK_CONFIG_KEY = "taskCdcPath";
    public static final String RE_INIT_TABLE_CONFIG_PATH = "%s/config/sybase2csv/task/%s/ext_sybasease.yaml";
    public static final String SYBASE_USE_TASK_CONFIG_DIR = "/config/sybase2csv/task/";

    public static final String SYBASE_USE_DATA_DIR = "/config/sybase2csv/data";

    public static final String SYBASE_USE_CSV_DIR = "/config/sybase2csv/csv";

    public static final String SYBASE_USE_TRACE_DIR = "/config/sybase2csv/trace";

    public static final String YAML_METADATA_NAME = "object_metadata.yaml";
}
