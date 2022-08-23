package com.tapdata.tm.autoinspect.connector;

import io.tapdata.entity.schema.TapTable;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/18 17:48 Create
 */
public interface IPdkConnector extends IConnector {

    TapTable getTapTable(String tableName);
}
