package com.tapdata.tm.commons.util;

import org.apache.commons.lang3.StringUtils;

/**
 * @Author: Zed
 * @Date: 2021/10/13
 * @Description:
 */
public enum MetaType {
    collection,
    view,
    table,
    mongo_view,
    job,
    ftp,
    file,
    directory,
    dataflow,
    database,
    api,
    apiendpoint,
    processor_node,

    VikaDatasheet,
    qingFlowApp

    ;

    public static boolean isView(String type) {
        if (StringUtils.isBlank(type)) {
            return false;
        }
        type = type.trim().toLowerCase();
        return MetaType.view.name().equals(type) || MetaType.mongo_view.name().equals(type);
    }
}
