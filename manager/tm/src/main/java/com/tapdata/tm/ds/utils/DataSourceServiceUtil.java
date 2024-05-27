package com.tapdata.tm.ds.utils;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;

public class DataSourceServiceUtil {
    private DataSourceServiceUtil() {

    }

    public static void setAccessNodeInfoFromOldConnectionDto(DataSourceConnectionDto oldConnection, DataSourceConnectionDto update) {
        if (null == oldConnection){
            return;
        }
        if (null == update) {
            return;
        }
        if (null == update.getAccessNodeType()) {
            update.setAccessNodeType(oldConnection.getAccessNodeType());
        }
        if (null == update.getAccessNodeProcessId()) {
            update.setAccessNodeProcessId(oldConnection.getAccessNodeProcessId());
        }
    }
}
