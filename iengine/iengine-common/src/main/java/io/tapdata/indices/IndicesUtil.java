package io.tapdata.indices;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.model.IndexOptions;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.RelateDatabaseField;
import com.tapdata.entity.TableIndex;
import com.tapdata.entity.TableIndexColumn;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.indices.impl.MongodbIndicesImpl;
import io.tapdata.indices.impl.MssqlIndicesImpl;
import io.tapdata.indices.impl.MysqlIndicesImpl;
import io.tapdata.indices.impl.OracleIndicesImpl;
import io.tapdata.indices.impl.PostgresqlIndicesImpl;
import io.tapdata.schema.SchemaList;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.util.Assert;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * 索引工具 - 索引操作入口
 * <pre>
 * Author: <a href="mailto:linhs@thoughtup.cn">Harsen</a>
 * CreateTime: 2021/4/13 上午11:31
 * </pre>
 */
public class IndicesUtil {
	private final static Logger logger = LogManager.getLogger(IndicesUtil.class);

	private final static String ERR_INDEX_EXIST = "Index exists {}";
	public final static String ERR_INDEX_CREATE = "Create index error {}\n  {}";

	public final static IIndices<Connection> MYSQL = new MysqlIndicesImpl();
	public final static IIndices<Connection> MSSQL = new MssqlIndicesImpl();
	public final static IIndices<Connection> ORACLE = new OracleIndicesImpl();
	public final static IIndices<Connection> POSTGRESQL = new PostgresqlIndicesImpl();
	public final static IIndices<Connection> GREENPLUM = new PostgresqlIndicesImpl();
	public final static IIndices<MongoClient> MONGODB = new MongodbIndicesImpl();

	/**
	 * 获取索引操作实例
	 *
	 * @param databaseType 数据库类型
	 * @param <T>          连接类型
	 * @return 索引操作实例
	 */
	public static <T> IIndices<T> getInstance(DatabaseTypeEnum databaseType) throws NotFoundException {
		switch (databaseType) {
			case MARIADB:
			case MYSQL:
			case MYSQL_PXC:
			case KUNDB:
			case ADB_MYSQL:
			case ALIYUN_MYSQL:
			case ALIYUN_MARIADB:
			case TIDB:
				return (IIndices<T>) MYSQL;
			case MSSQL:
			case ALIYUN_MSSQL:
				return (IIndices<T>) MSSQL;
			case ORACLE:
				return (IIndices<T>) ORACLE;
			case POSTGRESQL:
			case ALIYUN_POSTGRESQL:
			case ADB_POSTGRESQL:
				return (IIndices<T>) POSTGRESQL;
			case GREENPLUM:
				return (IIndices<T>) GREENPLUM;
			case MONGODB:
			case ALIYUN_MONGODB:
				return (IIndices<T>) MONGODB;
			default:
				throw new NotFoundException("Indices not found instance '" + databaseType.getType() + "'");
		}
	}

	/**
	 * 获取连接模式
	 *
	 * @param connections 连接
	 * @return 模式
	 */
	public static String getSchema(Connections connections) {
		DatabaseTypeEnum databaseType = DatabaseTypeEnum.fromString(connections.getDatabase_type());
		switch (databaseType) {
			case MSSQL:
			case ALIYUN_MSSQL:
			case POSTGRESQL:
			case ALIYUN_POSTGRESQL:
			case ADB_POSTGRESQL:
			case GREENPLUM:
			case ORACLE:
				return connections.getDatabase_owner();
			default:
				if ((databaseType == DatabaseTypeEnum.MONGODB || databaseType == DatabaseTypeEnum.ALIYUN_MONGODB) && StringUtils.isEmpty(connections.getDatabase_name())) {
					String databaseUri = connections.getDatabase_uri();
					if (StringUtils.isNotBlank(databaseUri)) {
						MongoClientURI uri = new MongoClientURI(databaseUri);
						connections.setDatabase_name(uri.getDatabase());
					}
				}
				return connections.getDatabase_name();
		}
	}

	/**
	 * 加载索引
	 *
	 * @param connections 连接配置
	 * @param connection  连接
	 * @param table       表信息
	 * @param <T>         连接类型
	 * @throws Exception 异常
	 */
	public static <T> void loadIndex(Connections connections, T connection, RelateDataBaseTable table) throws Exception {
		String schema = getSchema(connections);
		Assert.notNull(schema, "Connections not find attribute 'schema' " + connections);
		DatabaseTypeEnum databaseType = DatabaseTypeEnum.fromString(connections.getDatabase_type());
		Assert.notNull(databaseType, "DatabaseType not parse '" + connections.getDatabase_type() + "'");

		getInstance(databaseType).load(connection, schema, table);
	}

	public static <T> void loadAllIndex(Connections connections, T connection, Map<String, Map<String, TableIndex>> indexMap) throws Exception {
		String schema = getSchema(connections);
		Assert.notNull(schema, "Connections not find attribute 'schema' " + connections);
		DatabaseTypeEnum databaseType = DatabaseTypeEnum.fromString(connections.getDatabase_type());
		Assert.notNull(databaseType, "DatabaseType not parse '" + connections.getDatabase_type() + "'");

		getInstance(databaseType).loadAll(connection, schema, indexMap);
	}

	/**
	 * 创建索引
	 *
	 * @param databaseType 数据库类型
	 * @param connection   连接
	 * @param me           消息
	 * @param <T>          连接类型
	 * @throws Exception 异常
	 */
	public static <T> void createIndex(DatabaseTypeEnum databaseType, T connection, MessageEntity me) throws Exception {
		Assert.notNull(me.getAfter(), "MessageEntity not find attribute 'after'");
		String schema = (String) me.getAfter().get("schema");
		Assert.notNull(schema, "MessageEntity not find attribute 'after.schema'");
		Mapping mapping = me.getMapping();
		String tableName;
		if (mapping != null) {
			tableName = mapping.getTo_table();
		} else {
			tableName = me.getTableName();
		}
		TableIndex tableIndex = getTableIndex(me);
		IIndices<Object> instance = getInstance(databaseType);
		if (instance.exist(connection, schema, tableName, tableIndex.getIndexName())) {
			if (logger.isDebugEnabled()) {
				logger.debug(ERR_INDEX_EXIST, tableIndex);
			}
		} else {
			instance.create(connection, schema, tableName, tableIndex);
		}
	}

	/**
	 * 创建索引
	 *
	 * @param databaseTypeStr 数据库类型
	 * @param connection      连接
	 * @param me              消息
	 * @param <T>             连接类型
	 * @throws Exception 异常
	 */
	public static <T> void createIndex(String databaseTypeStr, T connection, MessageEntity me) throws Exception {
		DatabaseTypeEnum databaseType = DatabaseTypeEnum.fromString(databaseTypeStr);
		createIndex(databaseType, connection, me);
	}

	/**
	 * 创建MongoDB索引
	 *
	 * @param clientOperator 客户端操作
	 * @param me             消息
	 */
	public static void createIndex(ClientMongoOperator clientOperator, MessageEntity me) throws Exception {
		String tableName = me.getMapping().getTo_table();
		TableIndex ti = getTableIndex(me);

		if (clientOperator.existIndex(tableName, ti.getIndexName())) {
			logger.info(ERR_INDEX_EXIST, ti);
			return;
		}

		logger.info("Create index {}", ti);

		Document indexDoc = new Document();
		ti.getColumns().forEach(tableIndexColumn ->
				indexDoc.append(tableIndexColumn.getColumnName(),
						tableIndexColumn.getColumnValue() != null ?
								tableIndexColumn.getColumnValue() :
								(tableIndexColumn.getColumnIsAsc() ? 1 : -1)));
		IndexOptions indexOptions;
		if (StringUtils.isNotBlank(ti.getDbIndexDescriptionJson())) {
			String dbIndexDescriptionJson = ti.getDbIndexDescriptionJson();
			Document indexDescDoc = Document.parse(dbIndexDescriptionJson);
			indexOptions = MongodbUtil.buildIndexOptions(indexDescDoc);
		} else {
			indexOptions = new IndexOptions();
			indexOptions.unique(ti.isUnique());
			indexOptions.name(ti.getIndexName());
		}
		clientOperator.createIndex(tableName, indexDoc, indexOptions);
	}

	// ---------- 创建索引消息处理

	/**
	 * 生成创建索引消息
	 *
	 * @param needToCreateIndex     是否创建索引
	 * @param targetConn            目标连接
	 * @param tables                表名
	 * @param mappings              映射集合
	 * @param messageEntityConsumer 消息消费
	 * @param addEndMessage         添加结束消息
	 */
	public static void generateCreateMessageEntity(boolean needToCreateIndex, Connections targetConn, List<RelateDataBaseTable> tables,
												   List<Mapping> mappings, Consumer<MessageEntity> messageEntityConsumer, boolean addEndMessage,
												   Predicate<?> stop) {
		// 不支持列名修改，支持表名修改
		Set<String> allFields;
		Map<String, Object> afterMap;
		if (needToCreateIndex) {
			String schema = IndicesUtil.getSchema(targetConn);
			if (StringUtils.isBlank(schema)) {
				return;
			}
			for (Mapping mapping : mappings) {
				if (null != stop && stop.test(null)) {
					break;
				}
				RelateDataBaseTable table = ((SchemaList<String, RelateDataBaseTable>) tables).get(mapping.getFrom_table());
				if (table == null) continue;

				allFields = new HashSet<>();
				for (RelateDatabaseField filed : table.getFields()) {
					allFields.add(filed.getField_name().toLowerCase());
				}
				for (TableIndex ti : table.getIndices()) {
					if (null != stop && stop.test(null)) {
						break;
					}
					afterMap = new HashMap<>();
					afterMap.put("schema", schema);
					afterMap.put("tableIndex", ti);

					try {
						// todo: 支持 MSSQL 聚集创建，非 MSSQL 删除主键索引（其它库未支持）
//            if (!DatabaseTypeEnum.MSSQL.getType().equals(targetConn.getDatabase_type()) && ti.isPrimaryKey()) {
//              logger.info("Target is not mssql, ignore primary key index: {}", ti);
//              continue;
//            }
						// 过滤包含非法列的索引
						if (checkColumn(ti, allFields)) {
							messageEntityConsumer.accept(new MessageEntity(OperationType.CREATE_INDEX.getOp(), afterMap, mapping.getFrom_table()));
						} else {
							logger.warn("Generate index 'MessageEntity' error, has abnormal columns {}", ti);
						}
					} catch (Exception e) {
						logger.warn("Generate index 'MessageEntity' error {}", ti, e);
					}
				}
			}
		} else {
			logger.info("Ignore create index.");
		}

		if (addEndMessage) {
			MessageEntity me = new MessageEntity();
			me.setOp(OperationType.END_DDL.getOp());
			messageEntityConsumer.accept(me);
		}
	}

	/**
	 * 生成创建索引消息
	 *
	 * @param needToCreateIndex     是否创建索引
	 * @param targetConn            目标连接
	 * @param tables                表名
	 * @param mappings              映射集合
	 * @param messageEntityConsumer 消息消费
	 * @param addEndMessage         添加结束消息
	 */
	public static void generateCreateMessageEntityList(boolean needToCreateIndex, Connections targetConn, List<RelateDataBaseTable> tables,
													   List<Mapping> mappings, Consumer<List<MessageEntity>> messageEntityConsumer, boolean addEndMessage) {
		generateCreateMessageEntityList(needToCreateIndex, targetConn, tables, mappings, messageEntityConsumer, addEndMessage, null);
	}

	public static void generateCreateMessageEntityList(boolean needToCreateIndex, Connections targetConn, List<RelateDataBaseTable> tables,
													   List<Mapping> mappings, Consumer<List<MessageEntity>> messageEntityConsumer, boolean addEndMessage,
													   Predicate<?> stop) {
		List<MessageEntity> indices = new ArrayList<>();
		generateCreateMessageEntity(needToCreateIndex, targetConn, tables, mappings, indices::add, addEndMessage, stop);
		if (CollectionUtils.isNotEmpty(indices)) {
			messageEntityConsumer.accept(indices);
		}
	}

	/**
	 * 获取索引
	 *
	 * @param messageEntity 消息实体
	 * @return 索引
	 */
	public static TableIndex getTableIndex(MessageEntity messageEntity) {
		Map<String, Object> after = messageEntity.getAfter();
		Assert.notNull(after, String.format("Not found attribute 'after' of %s", messageEntity));
		TableIndex tableIndex = (TableIndex) after.get("tableIndex");
		Assert.notNull(tableIndex, String.format("Not found attribute 'after.tableIndex' of %s", messageEntity));
		return tableIndex;
	}

	/**
	 * add unique constraints for index
	 *
	 * @param tableIndex
	 * @param table
	 */
	public static void addUniqueConstraintIfNeeded(TableIndex tableIndex, RelateDataBaseTable table) {
		if (tableIndex == null || table == null) {
			return;
		}
		final List<String> conditionIndexCol = tableIndex.getColumns().stream().map(TableIndexColumn::getColumnName).collect(Collectors.toList());
		if (org.apache.commons.collections.CollectionUtils.isNotEmpty(table.getIndices())) {
			for (TableIndex index : table.getIndices()) {
				if (index.isUnique() && org.apache.commons.collections.CollectionUtils.isNotEmpty(index.getColumns())) {
					final List<String> indexColumns = index.getColumns().stream().map(TableIndexColumn::getColumnName).collect(Collectors.toList());
					if (org.apache.commons.collections.CollectionUtils.isEqualCollection(conditionIndexCol, indexColumns)) {
						tableIndex.setUnique(true);
						break;
					}
				}
			}
		}
	}

	// ---------- 以下是内部工具方法

	/**
	 * 检查索引列是否合法
	 *
	 * @param ti        索引
	 * @param allFields 所有列
	 * @return 全法状态
	 */
	private static boolean checkColumn(TableIndex ti, Set<String> allFields) {
		for (TableIndexColumn tic : ti.getColumns()) {
			if (allFields.contains(tic.getColumnName().toLowerCase())) {
				return true;
			}
		}
		return false;
	}

	public static class NotFoundException extends RuntimeException {
		/**
		 * Constructs a new runtime exception with {@code null} as its
		 * detail message.  The cause is not initialized, and may subsequently be
		 * initialized by a call to {@link #initCause}.
		 */
		public NotFoundException() {
			super();
		}

		/**
		 * Constructs a new runtime exception with the specified detail message.
		 * The cause is not initialized, and may subsequently be initialized by a
		 * call to {@link #initCause}.
		 *
		 * @param message the detail message. The detail message is saved for
		 *                later retrieval by the {@link #getMessage()} method.
		 */
		public NotFoundException(String message) {
			super(message);
		}

		/**
		 * Constructs a new runtime exception with the specified detail message and
		 * cause.  <p>Note that the detail message associated with
		 * {@code cause} is <i>not</i> automatically incorporated in
		 * this runtime exception's detail message.
		 *
		 * @param message the detail message (which is saved for later retrieval
		 *                by the {@link #getMessage()} method).
		 * @param cause   the cause (which is saved for later retrieval by the
		 *                {@link #getCause()} method).  (A <tt>null</tt> value is
		 *                permitted, and indicates that the cause is nonexistent or
		 *                unknown.)
		 * @since 1.4
		 */
		public NotFoundException(String message, Throwable cause) {
			super(message, cause);
		}

		/**
		 * Constructs a new runtime exception with the specified cause and a
		 * detail message of <tt>(cause==null ? null : cause.toString())</tt>
		 * (which typically contains the class and detail message of
		 * <tt>cause</tt>).  This constructor is useful for runtime exceptions
		 * that are little more than wrappers for other throwables.
		 *
		 * @param cause the cause (which is saved for later retrieval by the
		 *              {@link #getCause()} method).  (A <tt>null</tt> value is
		 *              permitted, and indicates that the cause is nonexistent or
		 *              unknown.)
		 * @since 1.4
		 */
		public NotFoundException(Throwable cause) {
			super(cause);
		}

		/**
		 * Constructs a new runtime exception with the specified detail
		 * message, cause, suppression enabled or disabled, and writable
		 * stack trace enabled or disabled.
		 *
		 * @param message            the detail message.
		 * @param cause              the cause.  (A {@code null} value is permitted,
		 *                           and indicates that the cause is nonexistent or unknown.)
		 * @param enableSuppression  whether or not suppression is enabled
		 *                           or disabled
		 * @param writableStackTrace whether or not the stack trace should
		 *                           be writable
		 * @since 1.7
		 */
		protected NotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
			super(message, cause, enableSuppression, writableStackTrace);
		}
	}
}
