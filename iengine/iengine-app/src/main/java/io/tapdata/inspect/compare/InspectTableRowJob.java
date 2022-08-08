package io.tapdata.inspect.compare;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CountOptions;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.inspect.InspectDataSource;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.ConverterProvider;
import io.tapdata.inspect.InspectJob;
import io.tapdata.inspect.InspectTaskContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 行校验父类
 * <pre>
 * Author: <a href="mailto:harsen_lin@163.com">Harsen</a>
 * CreateTime: 2021/8/10 下午3:29
 * </pre>
 */
public abstract class InspectTableRowJob extends InspectJob {

	private Logger logger = LogManager.getLogger(InspectTableRowJob.class);
	protected final ClientMongoOperator clientMongoOperator;

	protected int batchSize = 101;
	protected boolean excludeMongodbOid = false;

	public InspectTableRowJob(InspectTaskContext inspectTaskContext) {
		super(inspectTaskContext);
		this.clientMongoOperator = inspectTaskContext.getClientMongoOperator();
	}

	protected RdbmsResult findRdbms(Connections connections, ConverterProvider converterProvider, InspectDataSource inspectDataSource, boolean fullMatch, List<List<Object>> diffKeyValues) throws Exception {

//        long total = 0l;
//        List<Object> args = new ArrayList<>();
//        String sortColumn = inspectDataSource.getSortColumn();
//        List<String> sortColumns = getSortColumns(sortColumn);
//        String direction = inspectDataSource.getDirection();
//        direction = "DESC".equalsIgnoreCase(direction) ? " DESC" : " ASC";
//        String tableName = inspectDataSource.getTable();
//
//        Connection connection = null;
//        PreparedStatement ps = null;
//        ResultSet rs = null;
//        try {
//            connection = JdbcUtil.createConnection(connections);
//
//            if (!JdbcUtil.tableExists(connection, connections, tableName)) {
//                throw new Exception("Table " + tableName + " does not exists, connection name: " + connections.getName() + ", database type: " + connections.getDatabase_type());
//            }
//
//            DatabaseTypeEnum databaseTypeEnum = DatabaseTypeEnum.fromString(connections.getDatabase_type());
//            String sqlSelect = databaseTypeEnum.getSqlSelect();
//            sqlSelect = DatabaseTypeEnum.sqlSelectStringFormat(connections, sqlSelect, inspectDataSource.getTable());
//
//            if (inspectDataSource.getColumns() != null) {
//                if (!inspectDataSource.getColumns().contains(sortColumn)) {
//                    sortColumns.forEach(c -> inspectDataSource.getColumns().add(c));
//                }
//
//                //String selectFields = String.join(", ", inspectDataSource.getColumns());
//                String selectFields = inspectDataSource.getColumns()
//                    .stream()
//                    .map(c -> JdbcUtil.formatFieldName(c, connections.getDatabase_type()))
//                    .collect(Collectors.joining(", "));
//                sqlSelect = sqlSelect.replace("SELECT t.* FROM", "SELECT " + selectFields + " FROM");
//            } else {
//                if (fullMatch) {
//                    sqlSelect = sqlSelect.replace("SELECT t.* FROM", "SELECT * FROM");
//                } else {
//                    String selectFields = sortColumns.stream()
//                        .map(c -> JdbcUtil.formatFieldName(c, connections.getDatabase_type()))
//                        .collect(Collectors.joining(", "));
//                    sqlSelect = sqlSelect.replace("SELECT t.* FROM",
//                        "SELECT " + selectFields + " FROM");
//                }
//            }
//            sqlSelect = StringUtils.removeEnd(sqlSelect, "\\st");
//
//            String where = inspectDataSource.getWhere();
//            where = where != null ? where.trim() : "";
//            if (null != inspectDataSource.getInitialOffset()) {
//                where = where.replace("${OFFSET1}", inspectDataSource.getInitialOffset());
//            }
//
//            // 拼接差异结果条件：(k1=? and k2=? or k1=? and k2=? ...)
//            if (null != diffKeyValues && !diffKeyValues.isEmpty()) {
//                StringBuilder diffWhere = new StringBuilder();
//                if (where.toLowerCase().contains("where")) {
//                    diffWhere.append(" and ");
//                } else {
//                    diffWhere.append("where");
//                }
//
//                diffWhere.append("(");
//                List<Object> keyValues;
//                int sortColumnLen = sortColumns.size(), diffKeyValueLen = diffKeyValues.size();
//                for (int keyIdx = 0; keyIdx < diffKeyValueLen; keyIdx++) {
//                    keyValues = diffKeyValues.get(keyIdx);
//                    if (sortColumnLen != keyValues.size()) {
//                        logger.error("Inspect add detail condition error, sort column size({}) not equal to value size({})", sortColumnLen, keyValues.size());
//                        throw new RuntimeException(String.format("Inspect add detail condition error, sort column size(%s) not equal to value size(%s)", sortColumnLen, keyValues.size()));
//                    }
//                    if (keyIdx > 0) diffWhere.append(" or ");
//                    for (int i = 0; i < sortColumnLen; i++) {
//                        if (i > 0) diffWhere.append(" and ");
//                        diffWhere.append(JdbcUtil.formatFieldName(sortColumns.get(i), connections.getDatabase_type())).append("=?");
//                        args.add(converterProvider.targetValueConverter(keyValues.get(i)));
//                    }
//                }
//                diffWhere.append(")");
//
//                where += diffWhere.toString();
//            }
//            sqlSelect += " " + where;
//
//            StringBuilder orderByBuilder = new StringBuilder(" ORDER BY ");
//            String orderBy;
//            String columnPrefix = "";
//            String columnSuffix = "";
//            String finalDirection = direction;
//            // 创建一个新的sqlSortColumns，在sql查询之前，会修改里面的column属性(排序字符规则)
//            // 保留sortColumns以免导致后续比对的时候，无法正确获取主键的字段名
//            List<String> sqlSortColumns = new ArrayList<>(sortColumns);
//            switch (databaseTypeEnum) {
//
//                case ORACLE:
//                case DB2:
//
//                    columnPrefix = "\"";
//                    columnSuffix = "\"";
//                    break;
//
//                case MYSQL:
//                case MYSQL_PXC:
//                case KUNDB:
//                case ADB_MYSQL:
//                case ALIYUN_MYSQL:
//                case MARIADB:
//                case ALIYUN_MARIADB:
//
//                    try (
//                        ResultSet columnMetadata = JdbcUtil.getColumnMetadata(connection, connections.getDatabase_name(),
//                            connections.getDatabase_owner(), inspectDataSource.getTable(), "%")
//                    ) {
//                        Map<String, RelateDatabaseField> sortFieldMetadata = new HashMap<>();
//                        while (columnMetadata.next()) {
//                            String colName = columnMetadata.getString("COLUMN_NAME");
//                            if (sortColumns.contains(colName)) {
//                                int dataType = columnMetadata.getInt("DATA_TYPE");
//                                RelateDatabaseField relateDatabaseField = new RelateDatabaseField();
//                                relateDatabaseField.setDataType(dataType);
//                                sortFieldMetadata.put(colName, relateDatabaseField);
//                            }
//                        }
//                        sqlSortColumns = sqlSortColumns.stream().map(column -> {
//                            RelateDatabaseField relateDatabaseField = sortFieldMetadata.get(column);
//                            int dataType = relateDatabaseField.getDataType();
//                            switch (dataType) {
//                                case Types.CHAR:
//                                case Types.VARCHAR:
//                                case Types.NVARCHAR:
//                                case Types.LONGVARCHAR:
//                                case Types.LONGNVARCHAR:
//                                case Types.NCHAR:
//
//                                    column = "binary(`" + column + "`)";
//                                    break;
//
//                                default:
//
//                                    column = "`" + column + "`";
//                                    break;
//                            }
//                            return column;
//                        }).collect(Collectors.toList());
//                    }
//                    break;
//
//                case POSTGRESQL:
//                case ALIYUN_POSTGRESQL:
//                case GREENPLUM:
//                case ADB_POSTGRESQL:
//
//                    try (
//                        ResultSet columnMetadata = JdbcUtil.getColumnMetadata(connection, connections.getDatabase_name(),
//                            connections.getDatabase_owner(), inspectDataSource.getTable(), "%")
//                    ) {
//                        String collate = " collate \"C\" ";
//                        Map<String, RelateDatabaseField> sortFieldMetadata = new HashMap<>();
//                        while (columnMetadata.next()) {
//                            String colName = columnMetadata.getString("COLUMN_NAME");
//                            if (sortColumns.contains(colName)) {
//                                int dataType = columnMetadata.getInt("DATA_TYPE");
//                                RelateDatabaseField relateDatabaseField = new RelateDatabaseField();
//                                relateDatabaseField.setDataType(dataType);
//                                sortFieldMetadata.put(colName, relateDatabaseField);
//                            }
//                        }
//                        sqlSortColumns = sqlSortColumns.stream().map(column -> {
//                            RelateDatabaseField relateDatabaseField = sortFieldMetadata.get(column);
//                            int dataType = relateDatabaseField.getDataType();
//                            switch (dataType) {
//                                case Types.CHAR:
//                                case Types.VARCHAR:
//                                case Types.NVARCHAR:
//                                case Types.LONGVARCHAR:
//                                case Types.LONGNVARCHAR:
//                                case Types.NCHAR:
//
//                                    column = "\"" + column + "\"" + collate;
//                                    break;
//
//                                default:
//
//                                    column = "\"" + column + "\"";
//                                    break;
//                            }
//                            return column;
//                        }).collect(Collectors.toList());
//                    }
//                    break;
//
//                default:
//
//                    break;
//            }
//            String finalColumnPrefix = columnPrefix;
//            String finalColumnSuffix = columnSuffix;
//            sqlSortColumns.forEach(column -> orderByBuilder.append(finalColumnPrefix).append(column).append(finalColumnSuffix).append(finalDirection).append(","));
//            orderBy = orderByBuilder.toString();
//            orderBy = StringUtils.removeEnd(orderBy, ",");
//
//            if (sqlSelect.toUpperCase().startsWith("SELECT")) {
//                String countSql = "SELECT COUNT(1) " + sqlSelect.substring(sqlSelect.toUpperCase().indexOf("FROM"));
//
//                logger.debug("Query " + inspectDataSource.getTable() + " count by sql: " + countSql);
//                try (PreparedStatement ps1 = connection.prepareStatement(countSql)) {
//                    if (!args.isEmpty()) {
//                        for (int i = 0; i < args.size(); i++) {
//                            ps1.setObject(i + 1, args.get(i));
//                        }
//                    }
//                    try (ResultSet rs1 = ps1.executeQuery()) {
//                        if (rs1.next()) {
//                            total = rs1.getLong(1);
//                        }
//                    }
//                } catch (Exception e) {
//                    logger.error("Query error, sql: {}, err msg: {}", countSql, e.getMessage(), e);
//                    throw e;
//                }
//            }
//
//            String selectSql = sqlSelect + orderBy;
//            logger.info("Query " + inspectDataSource.getTable() + " by sql: " + selectSql);
//            ps = connection.prepareStatement(selectSql);
//            if (!StringUtils.equalsAnyIgnoreCase(connections.getDatabase_type(), DatabaseTypeEnum.ADB_POSTGRESQL.getType())) {
//                ps.setFetchSize(batchSize);
//            }
//            if (!args.isEmpty()) {
//                for (int i = 0; i < args.size(); i++) {
//                    ps.setObject(i + 1, args.get(i));
//                }
//            }
//
//            if (StringUtils.equalsAnyIgnoreCase(connections.getDatabase_type(), DatabaseTypeEnum.MYSQL.getType(), DatabaseTypeEnum.MYSQL_PXC.getType(), DatabaseTypeEnum.ADB_MYSQL.getType(), DatabaseTypeEnum.ALIYUN_MYSQL.getType())
//                && ps instanceof StatementImpl) {
//                long currentTimeMillis = System.currentTimeMillis();
//                logger.info("Detected database is mysql, will use streaming mode to select table[{}], Enabling streaming mode", inspectDataSource.getTable());
//                ((StatementImpl) ps).enableStreamingResults();
//                logger.info("Streaming mode on table[{}] is enabled, spend: {} ms", inspectDataSource.getTable(), (System.currentTimeMillis() - currentTimeMillis));
//            }
//            rs = ps.executeQuery();
//            ResultSetMetaData metaData = rs.getMetaData();
//            int columnCount = metaData.getColumnCount();
//            List columnNames = new ArrayList<>();
//            for (int i = 1; i <= columnCount; i++) {
//                columnNames.add(metaData.getColumnName(i));
//            }
//
//            sortColumns = sortColumns.stream().map(c -> c.toLowerCase()).collect(Collectors.toList());
//            RdbmsResult rdbmsResult = new RdbmsResult(sortColumns, connections, inspectDataSource.getTable(), converterProvider);
//            rdbmsResult.connection = connection;
//            rdbmsResult.statement = ps;
//            rdbmsResult.columnCount = columnCount;
//            rdbmsResult.total = total;
//            rdbmsResult.resultSet = rs;
//            rdbmsResult.metaData = metaData;
//            rdbmsResult.columnNames = columnNames;
//            rdbmsResult.columnCount = columnCount;
//
//            return rdbmsResult;
//        } catch (Exception e) {
//            JdbcUtil.closeQuietly(rs);
//            JdbcUtil.closeQuietly(ps);
//            JdbcUtil.closeQuietly(connection);
//            throw e;
//        }
		return null;
	}

	protected MongoResult findMongo(Connections connections, ConverterProvider converterProvider, InspectDataSource inspectDataSource, boolean fullMatch, List<List<Object>> diffKeyValues) throws Exception {

		String sortColumn = inspectDataSource.getSortColumn();
		List<String> sortColumns = getSortColumns(sortColumn);

		String dbname = MongodbUtil.getDatabase(connections);
		String collection = inspectDataSource.getTable();

		Document filterDocument;
		if (inspectDataSource.getWhere() != null) {
			filterDocument = Document.parse(inspectDataSource.getWhere());
		} else {
			filterDocument = new Document();
		}

		// 拼接差异结果条件：(k1=? and k2=? or k1=? and k2=? ...)
		if (null != diffKeyValues && !diffKeyValues.isEmpty()) {
			List<Document> andList = new ArrayList<>();
			if (!filterDocument.isEmpty()) {
				andList.add(filterDocument);
			}

			Object tmpVal;
			List<Document> orList;
			int sortColumnLen = sortColumns.size();
			for (List<Object> keyValues : diffKeyValues) {
				if (sortColumnLen != keyValues.size()) {
					String errMsg = String.format("Inspect add detail condition error, sort column size(%s) not equal to value size(%s)", sortColumnLen, keyValues.size());
					logger.error(errMsg);
					throw new RuntimeException(errMsg);
				}
				orList = new ArrayList<>();
				for (int i = 0; i < sortColumnLen; i++) {
					tmpVal = converterProvider.targetValueConverter(keyValues.get(i));
					if ("_id".equals(sortColumns.get(i)) && !(tmpVal instanceof ObjectId)) {
						tmpVal = new ObjectId(tmpVal.toString());
					}
					orList.add(new Document(sortColumns.get(i), tmpVal));
				}
				if (!orList.isEmpty()) {
					if (orList.size() == 1) {
						andList.add(orList.get(0));
					} else {
						andList.add(new Document("$and", orList));
					}
				}
			}

			if (!andList.isEmpty()) {
				if (andList.size() == 1) {
					filterDocument = andList.get(0);
				} else {
					filterDocument = new Document("$or", andList);
				}
			}
		}

		Document projectionDocument = new Document();
		if (inspectDataSource.getColumns() != null) {
			inspectDataSource.getColumns().forEach(colName -> projectionDocument.append(colName, 1));
			sortColumns.forEach(column -> {
				if (!inspectDataSource.getColumns().contains(column)) {
					projectionDocument.append(column, 1);
				}
			});
		} else {
			if (!fullMatch) {
				sortColumns.forEach(c -> projectionDocument.append(c, 1));
			}
		}
		if (projectionDocument.size() == 0) {
			projectionDocument.append("__tapd8", 0);
		}

		// TODO 等待前端提供列选择功能后删除
		if (excludeMongodbOid && !sortColumns.contains("_id")) {
			projectionDocument.append("_id", 0);
		}

		Document sortDocument = new Document();
		sortColumns.forEach(c -> sortDocument.append(c, "DESC".equalsIgnoreCase(inspectDataSource.getDirection()) ? -1 : 1));

		int limit = inspectDataSource.getLimit();
		int skip = inspectDataSource.getSkip();

		MongoClient client = MongodbUtil.createMongoClient(connections, MongodbUtil.getForJavaCoedcRegistry());
		MongoDatabase db = client.getDatabase(dbname);
		MongoCollection<Document> col = db.getCollection(collection);

		CountOptions countOpts = new CountOptions();
		if (limit > 0) countOpts.limit(limit);
		if (skip > 0) countOpts.skip(skip);
		long total = col.countDocuments(filterDocument, countOpts);

		FindIterable<Document> findIterable = col.find(filterDocument).maxTime(30, TimeUnit.MINUTES)
				.projection(projectionDocument).batchSize(batchSize);

		if (limit > 0) findIterable.limit(limit);
		if (skip > 0) findIterable.skip(skip);

		findIterable.sort(sortDocument);

		sortColumns = sortColumns.stream().map(c -> c.toLowerCase()).collect(Collectors.toList());
		MongoResult mongoResult = new MongoResult(sortColumns, connections, inspectDataSource.getTable(), converterProvider);
		mongoResult.total = total;
		mongoResult.mongoClient = client;
		mongoResult.mongoCursor = findIterable.iterator();

		return mongoResult;
	}

}
