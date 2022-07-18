package com.tapdata.entity;

import com.tapdata.constant.MapUtil;
import com.tapdata.constant.MongodbUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author samuel
 * @Description
 * @create 2020-09-03 14:50
 **/
public class CdcEvent implements Serializable {

	private static final long serialVersionUID = 6526416159899822071L;
	/**
	 * 操作时间
	 */
	private Long opDate;
	/**
	 * 记录时间
	 */
	private Long insertDate;
	/**
	 * 数据快照
	 */
	private Map<String, Object> before;
	private Map<String, Object> after;
	/**
	 * cdc事件
	 */
	private Map<String, Object> event;
	/**
	 * 操作(i-insert, u-update, d-delete, ddl-ddl)
	 */
	private String op;
	/**
	 * 来源
	 */
	private Source source;
	/**
	 * 更新的字段
	 */
	private Set<String> updateFields;
	private Set<String> removeFields;

	private boolean batchLast = false;

	public CdcEvent() {

	}

	public CdcEvent(boolean batchLast) {
		this.batchLast = batchLast;
	}

	public CdcEvent(MessageEntity messageEntity, Connections connections) throws InstantiationException, IllegalAccessException {
		this.opDate = messageEntity.getTimestamp();
		if (MapUtils.isNotEmpty(messageEntity.getBefore())) {
			Map<String, Object> newMap = new HashMap<>();
			MapUtil.deepCloneMap(messageEntity.getBefore(), newMap);
			this.before = newMap;
		}
		if (MapUtils.isNotEmpty(messageEntity.getAfter())) {
			Map<String, Object> newMap = new HashMap<>();
			MapUtil.deepCloneMap(messageEntity.getAfter(), newMap);
			this.after = newMap;
		}
		this.event = messageEntity.getCdcEvent();
		this.op = messageEntity.getOp();
		this.source = new Source().init(connections, messageEntity.getTableName());
		this.updateFields = messageEntity.getUpdateFields();
		this.removeFields = messageEntity.getRemoveFields();
		this.insertDate = System.currentTimeMillis();
	}

	public boolean isBatchLast() {
		return batchLast;
	}

	public void setBatchLast(boolean batchLast) {
		this.batchLast = batchLast;
	}

	public class Source {

		/**
		 * 连接ID
		 */
		private String connId;
		/**
		 * 数据库类型(DatabaseTypeEnum)
		 */
		private String type;
		/**
		 * 数据库uri
		 * if type=mongodb: mongodb://username:xxxxxxx@host:port/dbName
		 * if type=oracle,mysql,mssql...: host:port/dbName
		 * else empty
		 */
		private String uri;
		/**
		 * 地址
		 */
		private String host;
		/**
		 * 端口
		 */
		private Integer port;
		/**
		 * 库名
		 */
		private String dbName;
		/**
		 * schema
		 */
		private String schema;
		/**
		 * 用户名
		 */
		private String username;
		/**
		 * 资源名称(table, collection, file name...)
		 */
		private String name;

		private Source() {

		}

		private Source init(Connections connections, String sourceName) {
			if (connections == null) {
				throw new IllegalArgumentException("Missing input args connections");
			}
			if (StringUtils.isBlank(sourceName)) {
				throw new IllegalArgumentException("Missing input args sourceName");
			}

			this.connId = connections.getId();
			this.type = connections.getDatabase_type();
			this.uri = connections.getDatabase_uri();
			this.host = connections.getDatabase_host();
			this.port = connections.getDatabase_port();
			this.dbName = connections.getDatabase_name();
			this.schema = connections.getDatabase_owner();
			this.username = connections.getDatabase_username();
			this.name = sourceName;

			handleUri();

			return this;
		}

		private void handleUri() {
			DatabaseTypeEnum databaseTypeEnum = DatabaseTypeEnum.fromString(type);
			switch (databaseTypeEnum) {
				case MONGODB:
				case ALIYUN_MONGODB:
					this.uri = MongodbUtil.maskUriPassword(uri);
					break;
				case ORACLE:
				case MYSQL:
				case MARIADB:
				case DAMENG:
				case MYSQL_PXC:
				case MSSQL:
				case ALIYUN_MSSQL:
				case SYBASEASE:
				case KUNDB:
				case ADB_MYSQL:
				case ALIYUN_MYSQL:
				case ALIYUN_MARIADB:
					this.uri = this.host + ":" + this.port + "/" + this.dbName;
					break;
				default:
					break;
			}
		}
	}
}
