package io.tapdata.connector.mysql;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.kit.DbKit;
import io.tapdata.pdk.apis.charset.DatabaseCharset;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2022-04-26 20:09
 **/
public class MysqlSchemaLoader {
	private static final String TAG = MysqlSchemaLoader.class.getSimpleName();
	private static final String SELECT_TABLES = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_TYPE='BASE TABLE'";
	private static final String TABLE_NAME_IN = " AND TABLE_NAME IN(%s)";
	private static final String SELECT_COLUMNS = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME %s";
	private final static String SELECT_ALL_INDEX_SQL = "select *\n" +
			"from (select i.TABLE_NAME,\n" +
			"             i.INDEX_NAME,\n" +
			"             i.INDEX_TYPE,\n" +
			"             i.COLLATION,\n" +
			"             i.NON_UNIQUE,\n" +
			"             i.COLUMN_NAME,\n" +
			"             i.SEQ_IN_INDEX,\n" +
			"             (select k.CONSTRAINT_NAME\n" +
			"              from INFORMATION_SCHEMA.KEY_COLUMN_USAGE k\n" +
			"              where k.TABLE_SCHEMA = '%s'\n" +
			"                and k.TABLE_NAME = i.TABLE_NAME\n" +
			"                and i.INDEX_NAME = CONCAT(k.CONSTRAINT_NAME, '_idx')\n" +
			"                and i.COLUMN_NAME = k.COLUMN_NAME) CONSTRAINT_NAME\n" +
			"      from INFORMATION_SCHEMA.STATISTICS i\n" +
			"\n" +
			"      where i.TABLE_SCHEMA = '%s'\n" +
			"        and i.TABLE_NAME %s\n" +
			"        and i.INDEX_NAME <> 'PRIMARY') t\n" +
			"where t.CONSTRAINT_NAME is null";

	private static final String SELECT_TABLE_CHAR_SET_SQL = "SELECT\n" +
			" tab.TABLE_NAME AS TABLE_NAME,\n" +
			" charset_col.CHARACTER_SET_NAME AS CHAR_SET \n" +
			" FROM\n" +
			" INFORMATION_SCHEMA.TABLES AS tab\n" +
			" LEFT JOIN ( SELECT * FROM INFORMATION_SCHEMA.COLLATION_CHARACTER_SET_APPLICABILITY ) AS charset_col ON tab.TABLE_COLLATION = COLLATION_NAME \n" +
			" WHERE\n" +
			" TABLE_SCHEMA = '%s' \n" +
			" AND TABLE_TYPE = 'BASE TABLE'" +
			" AND tab.TABLE_NAME in ('%s')";

	private static final String ALL_CHARACTER_SET_SQL = "SELECT CHARACTER_SET_NAME,DESCRIPTION FROM INFORMATION_SCHEMA.CHARACTER_SETS ORDER BY MAXLEN DESC";


	private TapConnectionContext tapConnectionContext;
	private MysqlJdbcContext mysqlJdbcContext;

	public MysqlSchemaLoader(MysqlJdbcContext mysqlJdbcContext) {
		this.mysqlJdbcContext = mysqlJdbcContext;
		this.tapConnectionContext = mysqlJdbcContext.getTapConnectionContext();
	}

	public void discoverSchema1(List<String> filterTable, Consumer<List<TapTable>> consumer, int tableSize) throws Throwable {
		if (null == consumer) {
			throw new IllegalArgumentException("Consumer cannot be null");
		}
		DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		List<TapTable> tempList = new ArrayList<>();
		String sql = String.format(SELECT_TABLES, database);
		if (CollectionUtils.isNotEmpty(filterTable)) {
			filterTable = filterTable.stream().map(t -> "'" + t + "'").collect(Collectors.toList());
			String tableNameIn = String.join(",", filterTable);
			sql += String.format(TABLE_NAME_IN, tableNameIn);
		}
		mysqlJdbcContext.query(sql, tableRs -> {
			while (tableRs.next()) {
				TapTable tapTable = TapSimplify.table(tableRs.getString("TABLE_NAME"));
				try {
					discoverFields(tapConnectionContext, tapTable);
				} catch (Exception e) {
					TapLogger.error(TAG, "Discover columns failed, error msg: " + e.getMessage() + "\n" + TapSimplify.getStackString(e));
				}
				try {
					discoverIndexes(tapConnectionContext, tapTable);
				} catch (Throwable e) {
					TapLogger.error(TAG, "Discover indexes failed, error msg: " + e.getMessage() + "\n" + TapSimplify.getStackString(e));
				}
				tempList.add(tapTable);
				if (tempList.size() == tableSize) {
					consumer.accept(tempList);
					tempList.clear();
				}
			}
			if (CollectionUtils.isNotEmpty(tempList)) {
				consumer.accept(tempList);
				tempList.clear();
			}
		});
	}

	public void discoverSchema(List<String> filterTable, Consumer<List<TapTable>> consumer, int tableSize) throws Throwable {
		if (null == consumer) {
			throw new IllegalArgumentException("Consumer cannot be null");
		}

		DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");

		List<String> allTables = queryAllTables(database, filterTable);
		if (CollectionUtils.isEmpty(allTables)) {
			consumer.accept(null);
			return;
		}

		TableFieldTypesGenerator instance = InstanceFactory.instance(TableFieldTypesGenerator.class);
		DefaultExpressionMatchingMap dataTypesMap = tapConnectionContext.getSpecification().getDataTypesMap();

		try {
			List<List<String>> partition = Lists.partition(allTables, tableSize);
			partition.forEach(tables -> {
				String tableNames = StringUtils.join(tables, "','");
				List<DataMap> columnList = queryAllColumns(database, tableNames);
				List<DataMap> indexList = queryAllIndexes(database, tableNames);
				DataMap charsetMap = queryAllCharset(database, tableNames);

				Map<String, List<DataMap>> columnMap = Maps.newHashMap();
				if (CollectionUtils.isNotEmpty(columnList)) {
					columnMap = columnList.stream().collect(Collectors.groupingBy(t -> t.getString("TABLE_NAME")));
				}
				Map<String, List<DataMap>> indexMap = Maps.newHashMap();
				if (CollectionUtils.isNotEmpty(indexList)) {
					indexMap = indexList.stream().collect(Collectors.groupingBy(t -> t.getString("TABLE_NAME")));
				}

				Map<String, List<DataMap>> finalColumnMap = columnMap;
				Map<String, List<DataMap>> finalIndexMap = indexMap;

				List<TapTable> tempList = new ArrayList<>();
				tables.forEach(table -> {
					TapTable tapTable = TapSimplify.table(table);
					discoverFields(finalColumnMap.get(table), tapTable, instance, dataTypesMap);
					discoverIndexes(finalIndexMap.get(table), tapTable);
					tapTable.setCharset(charsetMap.getString(table));
					tempList.add(tapTable);
				});

				if (CollectionUtils.isNotEmpty(columnList)) {
					consumer.accept(tempList);
					tempList.clear();
				}
			});

		} catch (Exception e) {
			throw new Exception(e);
		}
	}

	private void discoverFields(TapConnectionContext connectionContext, TapTable tapTable) throws Throwable {
		AtomicInteger primaryPos = new AtomicInteger(1);
		DataMap connectionConfig = connectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		TableFieldTypesGenerator tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
		try {
			String tableName = new StringJoiner("=").add("'").add(tapTable.getId()).add("'").toString();
			mysqlJdbcContext.query(String.format(SELECT_COLUMNS, database, tableName), columnRs -> {
				while (columnRs.next()) {
					String columnName = columnRs.getString("COLUMN_NAME");
					String columnType = columnRs.getString("COLUMN_TYPE");
					TapField field = TapSimplify.field(columnName, columnType);
					tableFieldTypesGenerator.autoFill(field, connectionContext.getSpecification().getDataTypesMap());

					int ordinalPosition = columnRs.getInt("ORDINAL_POSITION");
					field.pos(ordinalPosition);

					String isNullable = columnRs.getString("IS_NULLABLE");
					field.nullable(isNullable.equals("YES"));

					Object columnKey = columnRs.getObject("COLUMN_KEY");
					if (columnKey instanceof String && columnKey.equals("PRI")) {
						field.primaryKeyPos(primaryPos.getAndIncrement());
					}

					String columnDefault = columnRs.getString("COLUMN_DEFAULT");
					field.defaultValue(columnDefault);
					tapTable.add(field);
				}
			});
		} catch (Exception e) {
			throw new Exception("Load column metadata error, table: " + database + "." + tapTable.getName() + "; Reason: " + e.getMessage(), e);
		}
	}

	private void discoverFields(List<DataMap> columnList, TapTable tapTable, TableFieldTypesGenerator tableFieldTypesGenerator,
								DefaultExpressionMatchingMap dataTypesMap) {
		AtomicInteger primaryPos = new AtomicInteger(1);

		if (CollectionUtils.isEmpty(columnList)) {
			return;
		}
		columnList.forEach(dataMap -> {
			String columnName = dataMap.getString("COLUMN_NAME");
			String columnType = dataMap.getString("COLUMN_TYPE");
			TapField field = TapSimplify.field(columnName, columnType);
			tableFieldTypesGenerator.autoFill(field, dataTypesMap);

			int ordinalPosition = Integer.parseInt(dataMap.getString("ORDINAL_POSITION"));
			field.pos(ordinalPosition);

			String isNullable = dataMap.getString("IS_NULLABLE");
			field.nullable(isNullable.equals("YES"));

			Object columnKey = dataMap.getObject("COLUMN_KEY");
			if (columnKey instanceof String && columnKey.equals("PRI")) {
				field.primaryKeyPos(primaryPos.getAndIncrement());
			}

//			String columnDefault = dataMap.getString("COLUMN_DEFAULT");
//			field.defaultValue(columnDefault);
			tapTable.add(field);
		});
	}

	private List<DataMap> queryAllColumns(String database, String tableNames) {
		TapLogger.debug(TAG, "Query all columns, database: {}, tableNames:{}", database, tableNames);

		String inTableName = new StringJoiner(tableNames).add("IN ('").add("')").toString();
		String sql = String.format(SELECT_COLUMNS, database, inTableName);
		List<DataMap> columnList = TapSimplify.list();
		try {
			mysqlJdbcContext.query(sql, resultSet -> {
				List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
				while (resultSet.next()) {
					columnList.add(DbKit.getRowFromResultSet(resultSet, columnNames));
				}
			});
		} catch (Throwable e) {
			TapLogger.error(TAG, "Execute queryAllColumns failed, error: " + e.getMessage(), e);
		}
		return columnList;
	}

	/**
	 * Returns after obtaining the character sets of multiple tables
	 * @Author Gavin
	 * @Date 2022-9-15:00
	 * */
	private DataMap queryAllCharset(String database, String tableNames) {
		TapLogger.debug(TAG, "Query all charset, database: {}, tableNames:{}", database, tableNames);
		String sql = String.format(SELECT_TABLE_CHAR_SET_SQL, database, tableNames);
		AtomicReference<DataMap> charsetList = new AtomicReference<>();//
		try {
			charsetList.set(DataMap.create());
			mysqlJdbcContext.query(sql, resultSet -> {
				while (resultSet.next()) {
					charsetList.get().put(resultSet.getString("TABLE_NAME"),resultSet.getString("CHAR_SET"));
				}
			});
		} catch (Throwable e) {
			TapLogger.error(TAG, "Execute queryAllCharset failed, error: " + e.getMessage(), e);
		}
		return charsetList.get();
	}

	private List<DataMap> queryAllIndexes(String database, String tableNames) {
		TapLogger.debug(TAG, "Query all indexes, database: {}, tableNames:{}", database, tableNames);
		List<DataMap> indexList = TapSimplify.list();

		String inTableName = new StringJoiner(tableNames).add("IN ('").add("')").toString();
		String sql = String.format(SELECT_ALL_INDEX_SQL, database, database, inTableName);
		try {
			mysqlJdbcContext.query(sql, resultSet -> {
				List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
				while (resultSet.next()) {
					indexList.add(DbKit.getRowFromResultSet(resultSet, columnNames));
				}
			});
		} catch (Throwable e) {
			TapLogger.error(TAG, "Execute queryAllIndexes failed, error: " + e.getMessage(), e);
		}
		return indexList;
	}

	private void discoverIndexes(TapConnectionContext tapConnectionContext, TapTable tapTable) throws Throwable {
		DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		List<TapIndex> indexes = new ArrayList<>();
		mysqlJdbcContext.query(String.format(SELECT_ALL_INDEX_SQL, database, database, tapTable.getId()), indexRs -> {
			while (indexRs.next()) {
				String indexName = indexRs.getString("INDEX_NAME");
				TapIndex tapIndex = indexes.stream().filter(i -> i.getName().equals(indexName)).findFirst().orElse(null);
				if (null == tapIndex) {
					tapIndex = new TapIndex();
					tapIndex.setName(indexName);
					int nonUnique = indexRs.getInt("NON_UNIQUE");
					tapIndex.setUnique(nonUnique == 1);
					tapIndex.setPrimary(false);
					indexes.add(tapIndex);
				}
				List<TapIndexField> indexFields = tapIndex.getIndexFields();
				if (null == indexFields) {
					indexFields = new ArrayList<>();
					tapIndex.setIndexFields(indexFields);
				}
				TapIndexField tapIndexField = new TapIndexField();
				tapIndexField.setName(indexRs.getString("COLUMN_NAME"));
				String collation = indexRs.getString("COLLATION");
				tapIndexField.setFieldAsc("A".equals(collation));
				indexFields.add(tapIndexField);
			}
		});
		tapTable.setIndexList(indexes);
	}

	private void discoverIndexes(List<DataMap> indexList, TapTable tapTable) {
		List<TapIndex> indexes = new ArrayList<>();

		if (CollectionUtils.isEmpty(indexList)) {
			return;
		}
		indexList.forEach(dataMap -> {
			String indexName = dataMap.getString("INDEX_NAME");
			TapIndex tapIndex = indexes.stream().filter(i -> i.getName().equals(indexName)).findFirst().orElse(null);
			if (null == tapIndex) {
				tapIndex = new TapIndex();
				tapIndex.setName(indexName);
				int nonUnique = Integer.parseInt(dataMap.getString("NON_UNIQUE"));
				tapIndex.setUnique(nonUnique == 1);
				tapIndex.setPrimary(false);
				indexes.add(tapIndex);
			}
			List<TapIndexField> indexFields = tapIndex.getIndexFields();
			if (null == indexFields) {
				indexFields = new ArrayList<>();
				tapIndex.setIndexFields(indexFields);
			}
			TapIndexField tapIndexField = new TapIndexField();
			tapIndexField.setName(dataMap.getString("COLUMN_NAME"));
			String collation = dataMap.getString("COLLATION");
			tapIndexField.setFieldAsc("A".equals(collation));
			indexFields.add(tapIndexField);
		});
		tapTable.setIndexList(indexes);

	}

	private List<String> queryAllTables(String database, List<String> filterTable) {
		String sql = String.format(SELECT_TABLES, database);
		if (CollectionUtils.isNotEmpty(filterTable)) {
			filterTable = filterTable.stream().map(t -> "'" + t + "'").collect(Collectors.toList());
			String tableNameIn = String.join(",", filterTable);
			sql += String.format(TABLE_NAME_IN, tableNameIn);
		}

		List<String> tableList = TapSimplify.list();
		try {
			mysqlJdbcContext.query(sql, resultSet -> {
				while (resultSet.next()) {
					tableList.add(resultSet.getString("TABLE_NAME"));
				}
			});
		} catch (Throwable e) {
			TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
		}
		return tableList;
	}

	public void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) {
		DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		String sql = String.format(SELECT_TABLES, database);
		List<String> list = new ArrayList<>();
		try {
			mysqlJdbcContext.query(sql, rs -> {
				while (rs.next()) {
					list.add(rs.getString("TABLE_NAME"));
					if (list.size() >= batchSize) {
						listConsumer.accept(list);
						list.clear();
					}
				}
			});
		} catch (Throwable e) {
			throw new RuntimeException("Get table names failed, sql: " + sql + ", error: " + e.getMessage(), e);
		}
		if (list.size() > 0) {
			listConsumer.accept(list);
			list.clear();
		}
	}

	/**
	 * 获取数据库支持的全部字符集，返回结果包括：字符集名称+字符集描述
	 * Get all the character sets supported by the database.
	 * The returned results include:
	 *     character set name and charset description
	 * @Author Gavin
	 * @Date 2022-9-17:00
	 * */
	public List<DatabaseCharset> getAllCharSet() throws Throwable {
		List<DatabaseCharset> tabCharsetList = new ArrayList<>();
		mysqlJdbcContext.query(ALL_CHARACTER_SET_SQL,result->{
			while (result.next()){
				DatabaseCharset databaseCharset
						= DatabaseCharset.create()
						.charset(result.getString("CHARACTER_SET_NAME"))
						.description(result.getString("DESCRIPTION") );
				tabCharsetList.add(databaseCharset);
			}
		});
		return tabCharsetList;
	}
}
