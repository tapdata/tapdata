package io.tapdata.connector.mysql.ddl.ccj;

import io.tapdata.common.ddl.alias.DbDataTypeAlias;
import io.tapdata.common.ddl.ccj.CCJBaseDDLWrapper;

public abstract class MysqlDDLWrapper extends CCJBaseDDLWrapper {

    @Override
    protected String getDataTypeFromAlias(String alias) {
        return new DbDataTypeAlias(alias).toDataType();
    }

}
