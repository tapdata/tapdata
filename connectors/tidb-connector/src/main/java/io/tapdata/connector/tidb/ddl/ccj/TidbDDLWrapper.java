package io.tapdata.connector.tidb.ddl.ccj;

import io.tapdata.common.ddl.ccj.CCJBaseDDLWrapper;

public abstract class TidbDDLWrapper extends CCJBaseDDLWrapper {

    @Override
    protected String getDataTypeFromAlias(String s) {
        return s;
    }

}
