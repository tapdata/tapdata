package io.tapdata.milestone;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.DatabaseTypeEnum;

import java.util.Map;

/**
 * @author samuel
 * @Description 里程碑枚举类，记录所有里程碑
 * @create 2020-12-23 12:28
 **/
public enum MilestoneStage {

	/* 初始化编排 */
	INIT_DATAFLOW(new DatabaseTypeEnum[]{}, new DatabaseTypeEnum[]{}, new String[]{}, new String[]{}, false, MilestoneGroup.INIT),

	/* 初始化采集器 */
	INIT_CONNECTOR(new DatabaseTypeEnum[]{}, new DatabaseTypeEnum[]{}, new String[]{}, new String[]{}, false, MilestoneGroup.INIT),

	/* 初始化写入线程 */
	INIT_TRANSFORMER(new DatabaseTypeEnum[]{}, new DatabaseTypeEnum[]{}, new String[]{}, new String[]{}, false, MilestoneGroup.INIT),

	/* 连接源端 */
	CONNECT_TO_SOURCE(new DatabaseTypeEnum[]{
			DatabaseTypeEnum.ORACLE, DatabaseTypeEnum.MSSQL, DatabaseTypeEnum.ALIYUN_MSSQL, DatabaseTypeEnum.POSTGRESQL, DatabaseTypeEnum.ALIYUN_POSTGRESQL, DatabaseTypeEnum.GREENPLUM, DatabaseTypeEnum.MYSQL, DatabaseTypeEnum.DB2, DatabaseTypeEnum.GAUSSDB200, DatabaseTypeEnum.GBASE8S, DatabaseTypeEnum.MONGODB, DatabaseTypeEnum.ALIYUN_MONGODB,
			DatabaseTypeEnum.ADB_POSTGRESQL, DatabaseTypeEnum.KUNDB, DatabaseTypeEnum.ADB_MYSQL, DatabaseTypeEnum.ALIYUN_MYSQL
	}, new DatabaseTypeEnum[]{
			DatabaseTypeEnum.ORACLE, DatabaseTypeEnum.MSSQL, DatabaseTypeEnum.ALIYUN_MSSQL, DatabaseTypeEnum.POSTGRESQL, DatabaseTypeEnum.ALIYUN_POSTGRESQL, DatabaseTypeEnum.GREENPLUM, DatabaseTypeEnum.MYSQL, DatabaseTypeEnum.DB2, DatabaseTypeEnum.GAUSSDB200, DatabaseTypeEnum.GBASE8S, DatabaseTypeEnum.MONGODB, DatabaseTypeEnum.ALIYUN_MONGODB,
			DatabaseTypeEnum.HANA, DatabaseTypeEnum.ADB_POSTGRESQL, DatabaseTypeEnum.KUNDB, DatabaseTypeEnum.ADB_MYSQL, DatabaseTypeEnum.ALIYUN_MYSQL}, new String[]{}, new String[]{}, false, MilestoneGroup.INIT),

	/* 连接目标端 */
	CONNECT_TO_TARGET(new DatabaseTypeEnum[]{
			DatabaseTypeEnum.ORACLE, DatabaseTypeEnum.MSSQL, DatabaseTypeEnum.ALIYUN_MSSQL, DatabaseTypeEnum.POSTGRESQL, DatabaseTypeEnum.ALIYUN_POSTGRESQL, DatabaseTypeEnum.GREENPLUM, DatabaseTypeEnum.MYSQL, DatabaseTypeEnum.DB2, DatabaseTypeEnum.GAUSSDB200, DatabaseTypeEnum.GBASE8S, DatabaseTypeEnum.MONGODB, DatabaseTypeEnum.ALIYUN_MONGODB,
			DatabaseTypeEnum.ADB_POSTGRESQL, DatabaseTypeEnum.KUNDB, DatabaseTypeEnum.ADB_MYSQL, DatabaseTypeEnum.ALIYUN_MYSQL
	}, new DatabaseTypeEnum[]{
			DatabaseTypeEnum.ORACLE, DatabaseTypeEnum.MSSQL, DatabaseTypeEnum.ALIYUN_MSSQL, DatabaseTypeEnum.POSTGRESQL, DatabaseTypeEnum.ALIYUN_POSTGRESQL, DatabaseTypeEnum.GREENPLUM, DatabaseTypeEnum.MYSQL, DatabaseTypeEnum.DB2, DatabaseTypeEnum.GAUSSDB200, DatabaseTypeEnum.GBASE8S, DatabaseTypeEnum.MONGODB, DatabaseTypeEnum.ALIYUN_MONGODB, DatabaseTypeEnum.HANA,
			DatabaseTypeEnum.ADB_POSTGRESQL, DatabaseTypeEnum.KUNDB, DatabaseTypeEnum.ADB_MYSQL, DatabaseTypeEnum.ALIYUN_MYSQL
	}, new String[]{}, new String[]{}, false, MilestoneGroup.INIT),

	/* 同步前删除目标表、索引、函数、存储过程 */
	DROP_TARGET_SCHEMA(new DatabaseTypeEnum[]{DatabaseTypeEnum.MYSQL, DatabaseTypeEnum.KUNDB, DatabaseTypeEnum.ADB_MYSQL, DatabaseTypeEnum.ALIYUN_MYSQL}, true,
			new DatabaseTypeEnum[]{DatabaseTypeEnum.MYSQL, DatabaseTypeEnum.KUNDB, DatabaseTypeEnum.ADB_MYSQL, DatabaseTypeEnum.ALIYUN_MYSQL}, true,
			new String[]{},
			new String[]{ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE}, true, true, MilestoneGroup.STRUCTURE),

	/* 读取源端DDL */
	READ_SOURCE_DDL(new DatabaseTypeEnum[]{DatabaseTypeEnum.MYSQL, DatabaseTypeEnum.KUNDB, DatabaseTypeEnum.ADB_MYSQL}, true,
			new DatabaseTypeEnum[]{DatabaseTypeEnum.MYSQL, DatabaseTypeEnum.KUNDB, DatabaseTypeEnum.ADB_MYSQL, DatabaseTypeEnum.ALIYUN_MYSQL}, true,
			new String[]{ConnectorConstant.SYNC_TYPE_INITIAL_SYNC, ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC},
			new String[]{ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE}, true, true, MilestoneGroup.STRUCTURE),

	/* 同步前自动创建目标表 */
	CREATE_TARGET_TABLE(new DatabaseTypeEnum[]{},
			new DatabaseTypeEnum[]{DatabaseTypeEnum.ORACLE, DatabaseTypeEnum.MYSQL, DatabaseTypeEnum.MSSQL, DatabaseTypeEnum.ALIYUN_MSSQL, DatabaseTypeEnum.POSTGRESQL, DatabaseTypeEnum.ALIYUN_POSTGRESQL, DatabaseTypeEnum.GREENPLUM, DatabaseTypeEnum.MARIADB, DatabaseTypeEnum.MYSQL_PXC, DatabaseTypeEnum.DAMENG, DatabaseTypeEnum.HANA,
					DatabaseTypeEnum.ADB_POSTGRESQL, DatabaseTypeEnum.KUNDB, DatabaseTypeEnum.ADB_MYSQL, DatabaseTypeEnum.ALIYUN_MYSQL, DatabaseTypeEnum.ALIYUN_MARIADB},
			new String[]{ConnectorConstant.SYNC_TYPE_INITIAL_SYNC, ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC},
			new String[]{}, true, MilestoneGroup.STRUCTURE),

	/* 同步前清空目标表数据 */
	CLEAR_TARGET_DATA(new DatabaseTypeEnum[]{},
			new DatabaseTypeEnum[]{DatabaseTypeEnum.ORACLE, DatabaseTypeEnum.MYSQL, DatabaseTypeEnum.MSSQL, DatabaseTypeEnum.ALIYUN_MSSQL, DatabaseTypeEnum.POSTGRESQL, DatabaseTypeEnum.ALIYUN_POSTGRESQL,
					DatabaseTypeEnum.GREENPLUM, DatabaseTypeEnum.MONGODB, DatabaseTypeEnum.ALIYUN_MONGODB, DatabaseTypeEnum.MYSQL_PXC, DatabaseTypeEnum.DAMENG, DatabaseTypeEnum.ADB_POSTGRESQL, DatabaseTypeEnum.KUNDB, DatabaseTypeEnum.ADB_MYSQL, DatabaseTypeEnum.ALIYUN_MYSQL, DatabaseTypeEnum.HAZELCAST_CLOUD},
			new String[]{},
			new String[]{}, true, MilestoneGroup.STRUCTURE),

	/* 迁移场景，创建目标视图 */
	CREATE_TARGET_VIEW(new DatabaseTypeEnum[]{DatabaseTypeEnum.MYSQL, DatabaseTypeEnum.KUNDB, DatabaseTypeEnum.ADB_MYSQL, DatabaseTypeEnum.ALIYUN_MYSQL}, true,
			new DatabaseTypeEnum[]{DatabaseTypeEnum.MYSQL, DatabaseTypeEnum.KUNDB, DatabaseTypeEnum.ADB_MYSQL, DatabaseTypeEnum.ALIYUN_MYSQL}, true,
			new String[]{ConnectorConstant.SYNC_TYPE_INITIAL_SYNC, ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC},
			new String[]{ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE}, true, true, MilestoneGroup.STRUCTURE),

	/* 迁移场景，创建目标函数 */
	CREATE_TARGET_FUNCTION(new DatabaseTypeEnum[]{DatabaseTypeEnum.MYSQL, DatabaseTypeEnum.KUNDB, DatabaseTypeEnum.ADB_MYSQL, DatabaseTypeEnum.ALIYUN_MYSQL}, true,
			new DatabaseTypeEnum[]{DatabaseTypeEnum.MYSQL, DatabaseTypeEnum.KUNDB, DatabaseTypeEnum.ADB_MYSQL, DatabaseTypeEnum.ALIYUN_MYSQL}, true,
			new String[]{ConnectorConstant.SYNC_TYPE_INITIAL_SYNC, ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC},
			new String[]{ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE}, true, true, MilestoneGroup.STRUCTURE),

	/* 前一场景，创建目标存储过程 */
	CREATE_TARGET_PROCEDURE(new DatabaseTypeEnum[]{DatabaseTypeEnum.MYSQL, DatabaseTypeEnum.KUNDB, DatabaseTypeEnum.ADB_MYSQL, DatabaseTypeEnum.ALIYUN_MYSQL}, true,
			new DatabaseTypeEnum[]{DatabaseTypeEnum.MYSQL, DatabaseTypeEnum.KUNDB, DatabaseTypeEnum.ADB_MYSQL, DatabaseTypeEnum.ALIYUN_MYSQL}, true,
			new String[]{ConnectorConstant.SYNC_TYPE_INITIAL_SYNC, ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC},
			new String[]{ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE}, true, true, MilestoneGroup.STRUCTURE),

	/* 同步前给目标表创建索引 */
	CREATE_TARGET_INDEX(new DatabaseTypeEnum[]{},
			new DatabaseTypeEnum[]{DatabaseTypeEnum.ORACLE, DatabaseTypeEnum.MYSQL, DatabaseTypeEnum.MSSQL, DatabaseTypeEnum.ALIYUN_MSSQL, DatabaseTypeEnum.POSTGRESQL, DatabaseTypeEnum.ALIYUN_POSTGRESQL, DatabaseTypeEnum.GREENPLUM, DatabaseTypeEnum.MONGODB, DatabaseTypeEnum.ALIYUN_MONGODB, DatabaseTypeEnum.HANA,
					DatabaseTypeEnum.ADB_POSTGRESQL, DatabaseTypeEnum.KUNDB, DatabaseTypeEnum.ADB_MYSQL, DatabaseTypeEnum.ALIYUN_MYSQL},
			new String[]{ConnectorConstant.SYNC_TYPE_INITIAL_SYNC, ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC}, new String[]{}, true, MilestoneGroup.STRUCTURE),

	/* 全量读取数据快照 */
	READ_SNAPSHOT(new DatabaseTypeEnum[]{DatabaseTypeEnum.LOG_COLLECT, DatabaseTypeEnum.MEM_CACHE}, false,
			new DatabaseTypeEnum[]{}, true, new String[]{ConnectorConstant.SYNC_TYPE_INITIAL_SYNC, ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC}, new String[]{},
			true, false, MilestoneGroup.INITIAL_SYNC),

	/* 全量数据快照写入 */
	WRITE_SNAPSHOT(new DatabaseTypeEnum[]{DatabaseTypeEnum.LOG_COLLECT, DatabaseTypeEnum.MEM_CACHE}, false,
			new DatabaseTypeEnum[]{}, true, new String[]{ConnectorConstant.SYNC_TYPE_INITIAL_SYNC, ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC}, new String[]{},
			true, false, MilestoneGroup.INITIAL_SYNC),

	/* 进入增量读取模式 */
	READ_CDC_EVENT(new DatabaseTypeEnum[]{}, new DatabaseTypeEnum[]{}, new String[]{}, new String[]{}, false, MilestoneGroup.CDC),

	/* 进入增量写入模式 */
	WRITE_CDC_EVENT(new DatabaseTypeEnum[]{}, new DatabaseTypeEnum[]{}, new String[]{}, new String[]{}, false, MilestoneGroup.CDC),
	;

	/** 出现的条件，空则代表该条件无要求 **/
	/**
	 * 适应的源端数据库类型
	 */
	private DatabaseTypeEnum[] sourceDatabases;

	/**
	 * true - 包含的源库类型{@link MilestoneStage#sourceDatabases}
	 * false - 排除的源库类型{@link MilestoneStage#sourceDatabases}
	 */
	private boolean sourceTypeInclude = true;

	/**
	 * 适应的目标端数据类型
	 */
	private DatabaseTypeEnum[] targetDatabases;

	/**
	 * true - 包含的目标库类型{@link MilestoneStage#targetDatabases}
	 * false - 排除的目标库类型{@link MilestoneStage#targetDatabases}
	 */
	private boolean targetTypeInclude = true;

	/**
	 * 适应的同步模式(初始化、增量、初始化+增量)
	 * {@link ConnectorConstant}
	 */
	private String[] syncTypes;
	/**
	 * 适应的同步场景(迁移、同步)
	 * {@link ConnectorConstant}
	 */
	private String[] mappingTemplates;
	/**
	 * 是否强制要求offset为空
	 */
	private boolean needOffsetEmpty;
	/**
	 * 需要源端和目标端类型相同(同构数据库)
	 */
	private boolean needSameSourceAndTarget;

	/**
	 * 特殊的源端和目标端配对
	 * 比如：某些功能只有oracle->oracle, oracle->sqlserver才有
	 */
	private Map<DatabaseTypeEnum, DatabaseTypeEnum[]> sourceAndTargetSpecialMatch;

	private MilestoneGroup group;

	MilestoneStage(DatabaseTypeEnum[] sourceDatabases, DatabaseTypeEnum[] targetDatabases, String[] syncTypes, String[] mappingTemplates, boolean needOffsetEmpty, MilestoneGroup group) {
		this.sourceDatabases = sourceDatabases;
		this.targetDatabases = targetDatabases;
		this.syncTypes = syncTypes;
		this.mappingTemplates = mappingTemplates;
		this.needOffsetEmpty = needOffsetEmpty;
		this.group = group;
	}

	MilestoneStage(DatabaseTypeEnum[] sourceDatabases, boolean sourceTypeInclude, DatabaseTypeEnum[] targetDatabases, boolean targetTypeInclude,
				   String[] syncTypes, String[] mappingTemplates, boolean needOffsetEmpty, boolean needSameSourceAndTarget, MilestoneGroup group) {
		this.sourceDatabases = sourceDatabases;
		this.targetDatabases = targetDatabases;
		this.syncTypes = syncTypes;
		this.mappingTemplates = mappingTemplates;
		this.needOffsetEmpty = needOffsetEmpty;
		this.needSameSourceAndTarget = needSameSourceAndTarget;
		this.sourceTypeInclude = sourceTypeInclude;
		this.targetTypeInclude = targetTypeInclude;
		this.group = group;
	}

	public DatabaseTypeEnum[] getSourceDatabases() {
		return sourceDatabases;
	}

	public DatabaseTypeEnum[] getTargetDatabases() {
		return targetDatabases;
	}

	public String[] getSyncTypes() {
		return syncTypes;
	}

	public String[] getMappingTemplates() {
		return mappingTemplates;
	}

	public boolean isNeedOffsetEmpty() {
		return needOffsetEmpty;
	}

	public boolean isNeedSameSourceAndTarget() {
		return needSameSourceAndTarget;
	}

	public Map<DatabaseTypeEnum, DatabaseTypeEnum[]> getSourceAndTargetSpecialMatch() {
		return sourceAndTargetSpecialMatch;
	}

	public boolean isSourceTypeInclude() {
		return sourceTypeInclude;
	}

	public boolean isTargetTypeInclude() {
		return targetTypeInclude;
	}

	public MilestoneGroup getGroup() {
		return group;
	}
}
