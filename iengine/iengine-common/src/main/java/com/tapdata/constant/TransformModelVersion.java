package com.tapdata.constant;

import com.tapdata.entity.DatabaseTypeEnum;

import java.util.ArrayList;
import java.util.List;

/**
 * 模型推演版本信息
 */
public class TransformModelVersion {


	private static final List<DatabaseTypeEnum> v1SupportList = new ArrayList<>();

	static {
		v1SupportList.add(DatabaseTypeEnum.ORACLE);
		v1SupportList.add(DatabaseTypeEnum.DAMENG);
		v1SupportList.add(DatabaseTypeEnum.MYSQL);
		v1SupportList.add(DatabaseTypeEnum.MARIADB);
		v1SupportList.add(DatabaseTypeEnum.MYSQL_PXC);
		v1SupportList.add(DatabaseTypeEnum.TIDB);
		v1SupportList.add(DatabaseTypeEnum.KUNDB);
		v1SupportList.add(DatabaseTypeEnum.ADB_MYSQL);
		v1SupportList.add(DatabaseTypeEnum.MSSQL);
		v1SupportList.add(DatabaseTypeEnum.POSTGRESQL);
		v1SupportList.add(DatabaseTypeEnum.GREENPLUM);
		v1SupportList.add(DatabaseTypeEnum.ADB_POSTGRESQL);
		v1SupportList.add(DatabaseTypeEnum.GAUSSDB200);
		v1SupportList.add(DatabaseTypeEnum.HIVE);
		v1SupportList.add(DatabaseTypeEnum.HANA);

	}

	public static boolean isSupportV1(DatabaseTypeEnum databaseTypeEnum) {
		return v1SupportList.contains(databaseTypeEnum);
	}


}
