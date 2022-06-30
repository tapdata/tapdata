package io.tapdata.ddlp.converters;

import com.tapdata.entity.DatabaseTypeEnum;
import io.tapdata.annotation.DatabaseTypeAnnotation;
import io.tapdata.ddlp.utils.SqlDDLConverter;

/**
 * DDL转换器 - SQLServer
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/16 上午3:40 Create
 */
@DatabaseTypeAnnotation(type = DatabaseTypeEnum.MSSQL)
@DatabaseTypeAnnotation(type = DatabaseTypeEnum.ALIYUN_MSSQL)
public class MssqlDDLConverter extends SqlDDLConverter {

}
