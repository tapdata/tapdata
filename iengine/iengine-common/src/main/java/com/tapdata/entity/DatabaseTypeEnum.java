package com.tapdata.entity;

import io.tapdata.pdk.apis.entity.Capability;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Database type enum
 */
public enum DatabaseTypeEnum {
	MYSQL("mysql", "MySql", "SELECT t.* FROM `%s`.`%s` t", "SELECT count(*) FROM `%s`.`%s` t", true, true, "`%s`"),
	ORACLE("oracle", "Oracle", "SELECT t.* FROM %s.\"%s\" t", "SELECT count(*) FROM %s.\"%s\" t", true, true, "\"%s\""),
	MONGODB("mongodb", "MongoDB", false, false),
	MSSQL("sqlserver", "SQL Server", "SELECT t.* FROM \"%s\".\"%s\".\"%s\" t", "SELECT count(*) FROM \"%s\".\"%s\".\"%s\" t", true, true, "\"%s\""),
	SYBASEASE("sybase ase", "Sybase ASE", "SELECT t.* FROM [%s].[%s].[%s] t", "SELECT count(*) FROM [%s].[%s].[%s] t", true, true, "[%s]"),
	GRIDFS("gridfs", "GridFS", false, false),
	FILE("file", "File(s)", false, false),
	REST("rest api", "REST API", false, false),
	TCP_UDP("tcp_udp", "TCP/IP", false, false),
	DUMMY("dummy db", "Dummy DB", false, false),
	BITSFLOW("bitsflow", "Bitsflow", new String[]{"DK36"}, false, false),
	GBASE8S("gbase-8s", "GBase 8s", "SELECT t.* FROM %s.%s t", "SELECT count(*) FROM %s.%s t", true, true),
	CUSTOM("custom_connection", "Custom Connection", false, false),
	DB2("db2", "IBM Db2", "SELECT t.* FROM \"%s\".\"%s\" t", "SELECT count(*) FROM \"%s\".\"%s\" t", true, true, "\"%s\""),
	GAUSSDB200("gaussdb200", "GaussDB200", "SELECT t.* FROM \"%s\".\"%s\" t", "SELECT count(*) FROM \"%s\".\"%s\" t", true, true, "\"%s\""),
	POSTGRESQL("postgres", "PostgreSQL", "SELECT t.* FROM \"%s\".\"%s\".\"%s\" t", "SELECT count(*) FROM \"%s\".\"%s\" t", true, true, "\"%s\""),
	GREENPLUM("greenplum", "Greenplum", "SELECT t.* FROM \"%s\".\"%s\".\"%s\" t", "SELECT count(*) FROM \"%s\".\"%s\" t", true, true, "\"%s\""),
	ELASTICSEARCH("elasticsearch", "Elasticsearch", false, false),
	MEM_CACHE("mem_cache", "Memory Cache", false, false),
	LOG_COLLECT("log_collect", "Log Collect", false, false),
	LOG_COLLECT_V2("logCollector", "Log Collector", false, false),
	REDIS("redis", "Redis", false, false),
	MARIADB("mariadb", "MariaDB", "SELECT t.* FROM `%s`.`%s` t", "SELECT count(*) FROM `%s`.`%s` t", true, true, "`%s`"),
	KAFKA("kafka", "kafka", false, false),
	MQ("mq", "mq", false, false),
	MYSQL_PXC("mysql pxc", "MySql PXC", "SELECT t.* FROM `%s`.%s t", "SELECT count(*) FROM `%s`.%s t", true, true, "`%s`"),
	JIRA("jira", "JIAR REST API", false, false),
	DAMENG("dameng", "DaMeng", "SELECT t.* FROM \"%s\".\"%s\" t", "SELECT count(*) FROM \"%s\".\"%s\" t", true, true, "\"%s\""),
	HIVE("hive", "Hive", false, false),
	HBASE("hbase", "HBase", false, false),
	KUDU("kudu", "Kudu", false, false),
	HANA("hana", "SAP HANA", "SELECT t.* FROM \"%s\".\"%s\" t", "SELECT count(*) FROM \"%s\".\"%s\" t", true, true, "\"%s\""),
	TIDB("tidb", "TiDB", "SELECT t.* FROM `%s`.`%s` t", "SELECT count(*) FROM `%s`.`%s` t", true, true, "`%s`"),
	VIKA("vika", "Vika", false, false),
	CLICKHOUSE("clickhouse", "ClickHouse", "SELECT t.* FROM `%s`.`%s` t", "SELECT count(*) FROM `%s`.`%s` t", true, true, "`%s`"),
	KUNDB("kundb", "KunDB", "SELECT t.* FROM `%s`.`%s` t", "SELECT count(*) FROM `%s`.`%s` t", true, true, "`%s`"),
	ADB_MYSQL("adb_mysql", "ADB MySQL", "SELECT * FROM `%s`.`%s`", "SELECT count(*) FROM `%s`.`%s`", true, true, "`%s`"),
	ADB_POSTGRESQL("adb_postgres", "ADB PostgreSQL", "SELECT t.* FROM \"%s\".\"%s\" t", "SELECT count(*) FROM \"%s\".\"%s\" t", true, true, "\"%s\""),
	HAZELCAST_CLOUD("hazelcast_cloud_cluster", "Hazelcast Cloud Cluster", false, false),
	HAZELCAST_IMDG("hazelcastIMDG", "Hazelcast IMDG", false, false),
	ALIYUN_MYSQL("aliyun_mysql", "Aliyun MySql", "SELECT t.* FROM `%s`.`%s` t", "SELECT count(*) FROM `%s`.`%s` t", true, true, "`%s`"),
	ALIYUN_MARIADB("aliyun_mariadb", "Aliyun MariaDB", "SELECT t.* FROM `%s`.`%s` t", "SELECT count(*) FROM `%s`.`%s` t", true, true, "`%s`"),
	ALIYUN_MSSQL("aliyun_sqlserver", "Aliyun SQL Server", "SELECT t.* FROM \"%s\".\"%s\".\"%s\" t", "SELECT count(*) FROM \"%s\".\"%s\".\"%s\" t", true, true, "\"%s\""),
	ALIYUN_POSTGRESQL("aliyun_postgres", "Aliyun PostgreSQL", "SELECT t.* FROM \"%s\".\"%s\".\"%s\" t", "SELECT count(*) FROM \"%s\".\"%s\" t", true, true, "\"%s\""),
	ALIYUN_MONGODB("aliyun_mongodb", "Aliyun MongoDB", false, false),
	UNKNOWN("unknown", "unknown", false, false),
	;

	private String type;
	private String name;
	private String[] buildProfiles;
	private String sqlSelect;
	private String sqlSelectCount;
	private boolean needCreateTargetTable;
	private boolean rdbms;
	private String formatColumn;

	DatabaseTypeEnum(String type, String name, boolean needCreateTargetTable, boolean rdbms) {
		this.type = type;
		this.name = name;
		this.needCreateTargetTable = needCreateTargetTable;
		this.rdbms = rdbms;
	}

	DatabaseTypeEnum(String type, String name, String[] buildProfiles, boolean needCreateTargetTable, boolean rdbms) {
		this.type = type;
		this.name = name;
		this.buildProfiles = buildProfiles;
		this.needCreateTargetTable = needCreateTargetTable;
		this.rdbms = rdbms;
	}

	DatabaseTypeEnum(String type, String name, String sqlSelect, String sqlSelectCount, boolean needCreateTargetTable, boolean rdbms) {
		this.type = type;
		this.name = name;
		this.sqlSelect = sqlSelect;
		this.sqlSelectCount = sqlSelectCount;
		this.needCreateTargetTable = needCreateTargetTable;
		this.rdbms = rdbms;
	}

	DatabaseTypeEnum(String type, String name, String sqlSelect, String sqlSelectCount, boolean needCreateTargetTable, boolean rdbms, String formatColumn) {
		this.type = type;
		this.name = name;
		this.sqlSelect = sqlSelect;
		this.sqlSelectCount = sqlSelectCount;
		this.needCreateTargetTable = needCreateTargetTable;
		this.rdbms = rdbms;
		this.formatColumn = formatColumn;
	}

	public String getType() {
		return type;
	}

	public String getName() {
		return name;
	}

	public String[] getBuildProfiles() {
		return buildProfiles;
	}

	public String getSqlSelect() {
		return sqlSelect;
	}

	public String getSqlSelectCount() {
		return sqlSelectCount;
	}

	public boolean isNeedCreateTargetTable() {
		return needCreateTargetTable;
	}

	public static int getSqlSelectStringFormatCount(String sqlSelect) {
		if (StringUtils.isBlank(sqlSelect)) {
			return 0;
		}
		Pattern pattern = Pattern.compile("%s");
		Matcher matcher = pattern.matcher(sqlSelect);
		int count = 0;
		while (matcher.find()) {
			count++;
		}
		return count;
	}

	public static String sqlSelectStringFormat(Connections connections, String sqlSelect, String tableName) {
		if (connections == null || StringUtils.isBlank(sqlSelect) || StringUtils.isBlank(tableName)) return "";

		int sqlSelectStringFormatCount = getSqlSelectStringFormatCount(sqlSelect);
		if (sqlSelectStringFormatCount == 2) {
			if (StringUtils.equalsAny(
					connections.getDatabase_type(),
					DatabaseTypeEnum.MYSQL.getType(),
					DatabaseTypeEnum.MARIADB.getType(),
					DatabaseTypeEnum.MYSQL_PXC.getType(),
					DatabaseTypeEnum.TIDB.getType(),
					DatabaseTypeEnum.KUNDB.getType(),
					DatabaseTypeEnum.ADB_MYSQL.getType(),
					DatabaseTypeEnum.ALIYUN_MYSQL.getType(),
					DatabaseTypeEnum.ALIYUN_MARIADB.getType(),
					DatabaseTypeEnum.CLICKHOUSE.getType()
			)
			) {
				sqlSelect = String.format(sqlSelect, connections.getDatabase_name(), tableName);
			} else {
				sqlSelect = String.format(sqlSelect, connections.getDatabase_owner(), tableName);
			}
		} else if (sqlSelectStringFormatCount == 3) {
			sqlSelect = String.format(sqlSelect, connections.getDatabase_name(), connections.getDatabase_owner(), tableName);
		}

		return sqlSelect;
	}

	private static final Map<String, DatabaseTypeEnum> map = new HashMap<>();

	static {
		for (DatabaseTypeEnum databaseType : DatabaseTypeEnum.values()) {
			map.put(databaseType.getType(), databaseType);
		}
	}

	public static DatabaseTypeEnum fromString(String databaseType) {
		return map.getOrDefault(databaseType, UNKNOWN);
	}

	public enum DatabaseTypeVersion {
		VERSION_1_0("1.0"),
		VERSION_1_5("1.5"),
		VERSION_2_0("2.0");

		private String version;

		DatabaseTypeVersion(String version) {
			this.version = version;
		}

		public static DatabaseTypeVersion fromString(String version) {
			for (DatabaseTypeVersion connectionVersion : values()) {
				if (connectionVersion.version.equals(version)) {
					return connectionVersion;
				}
			}
			return null;
		}
	}

	public static class DatabaseType implements Serializable {

		private static final long serialVersionUID = -1955282464206383722L;
		private String type;

		private String name;

		private String[] buildProfiles;

		private List<String> supportTargetDatabaseType;

		private String id;
		/**
		 * pdk类型
		 */
		private String pdkType;
		private String icon;
		private String group;
		private Integer buildNumber;
		private String scope;
		private String jarFile;
		private Long jarTime;
		private String jarRid;
		private String version;
		private String pdkId;
		private String pdkHash;
		private List<Capability> capabilities;

		public DatabaseType() {
		}

		public DatabaseType(String type, String name, List<String> supportTargetDatabaseType) {
			this.type = type;
			this.name = name;
			this.supportTargetDatabaseType = supportTargetDatabaseType;
		}

		public DatabaseType(String type, String name, String[] buildProfiles, List<String> supportTargetDatabaseType) {
			this.type = type;
			this.name = name;
			this.buildProfiles = buildProfiles;
			this.supportTargetDatabaseType = supportTargetDatabaseType;
		}

		public List<Capability> getCapabilities() {
			return capabilities;
		}

		public void setCapabilities(List<Capability> capabilities) {
			this.capabilities = capabilities;
		}

		public String getType() {
			return type;
		}

		public String getName() {
			return name;
		}

		public List<String> getSupportTargetDatabaseType() {
			return supportTargetDatabaseType;
		}

		public String[] getBuildProfiles() {
			return buildProfiles;
		}

		public void setType(String type) {
			this.type = type;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setBuildProfiles(String[] buildProfiles) {
			this.buildProfiles = buildProfiles;
		}

		public void setSupportTargetDatabaseType(List<String> supportTargetDatabaseType) {
			this.supportTargetDatabaseType = supportTargetDatabaseType;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getPdkType() {
			return pdkType;
		}

		public void setPdkType(String pdkType) {
			this.pdkType = pdkType;
		}

		public String getIcon() {
			return icon;
		}

		public void setIcon(String icon) {
			this.icon = icon;
		}

		public String getGroup() {
			return group;
		}

		public void setGroup(String group) {
			this.group = group;
		}

		public Integer getBuildNumber() {
			return buildNumber;
		}

		public void setBuildNumber(Integer buildNumber) {
			this.buildNumber = buildNumber;
		}

		public String getScope() {
			return scope;
		}

		public void setScope(String scope) {
			this.scope = scope;
		}

		public String getJarFile() {
			return jarFile;
		}

		public void setJarFile(String jarFile) {
			this.jarFile = jarFile;
		}

		public Long getJarTime() {
			return jarTime;
		}

		public void setJarTime(Long jarTime) {
			this.jarTime = jarTime;
		}

		public String getJarRid() {
			return jarRid;
		}

		public void setJarRid(String jarRid) {
			this.jarRid = jarRid;
		}

		public String getVersion() {
			return version;
		}

		public void setVersion(String version) {
			this.version = version;
		}

		public String getPdkId() {
			return pdkId;
		}

		public void setPdkId(String pdkId) {
			this.pdkId = pdkId;
		}

		public String getPdkHash() {
			return pdkHash;
		}

		public void setPdkHash(String pdkHash) {
			this.pdkHash = pdkHash;
		}

		@Override
		public String toString() {
			return pdkId + "-" + group + "-" + version + "-" + scope;
		}
	}

	public static List<DatabaseTypeEnum> getRdbmsDatabaseTypes() {
		return Arrays.stream(values()).filter(e -> e.rdbms).collect(Collectors.toList());
	}

	public String getFormatColumn() {
		return formatColumn;
	}


}
