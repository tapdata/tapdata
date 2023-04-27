package io.tapdata.indices.impl;

import com.tapdata.constant.MD5Util;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.TableIndex;
import com.tapdata.entity.TableIndexColumn;
import com.tapdata.entity.TableIndexTypeEnums;
import io.tapdata.indices.IIndices;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC索引实现 - Oracle
 * <pre>
 * Author: <a href="mailto:linhs@thoughtup.cn">Harsen</a>
 * CreateTime: 2021/4/14 下午12:58
 * </pre>
 */
public class OracleIndicesImpl implements IIndices<Connection> {
	private final static Logger logger = LogManager.getLogger(OracleIndicesImpl.class);

	//CONSTRAINT_TYPE:
	// - C (check constraint on a table)
	// - P (primary key)
	// - U (unique key)
	// - R (referential integrity)
	// - V (with check option, on a view)
	// - O (with read only, on a view)
	private final static String SELECT_INDEX_SQL = "SELECT ind.INDEX_NAME, ind.INDEX_TYPE, ind.TABLE_NAME, ind.UNIQUENESS, ind_col.COLUMN_NAME, ind_col_ex.COLUMN_EXPRESSION, ind_col.COLUMN_POSITION, ind_col.DESCEND, uc.CONSTRAINT_TYPE\n" +
			" FROM sys.ALL_INDEXES ind\n" +
			" LEFT JOIN sys.ALL_IND_COLUMNS ind_col ON ind.OWNER = ind_col.INDEX_OWNER AND ind.INDEX_NAME = ind_col.INDEX_NAME\n" +
			" LEFT JOIN sys.ALL_IND_EXPRESSIONS ind_col_ex on ind.OWNER = ind_col_ex.INDEX_OWNER AND ind.INDEX_NAME = ind_col_ex.INDEX_NAME\n" +
			" LEFT JOIN sys.ALL_CONSTRAINTS uc ON uc.INDEX_NAME = ind_col.INDEX_NAME AND ind.OWNER = uc.OWNER\n" +
			"WHERE ind.TABLE_OWNER = ? AND ind.TABLE_TYPE = 'TABLE'\n" +
			"  AND (uc.CONSTRAINT_TYPE IS NULL OR uc.CONSTRAINT_TYPE='U' OR uc.CONSTRAINT_TYPE='C')\n" +
			"  AND ind.TABLE_NAME=?\n" +
			"ORDER BY ind.TABLE_OWNER,ind.TABLE_NAME,ind.INDEX_NAME,ind_col.COLUMN_POSITION";
	private final static String SELECT_INDEX_SQL_TABLE_NAMES = "SELECT ind.INDEX_NAME, ind.INDEX_TYPE, ind.TABLE_NAME, ind.UNIQUENESS, ind_col.COLUMN_NAME, ind_col_ex.COLUMN_EXPRESSION, ind_col.COLUMN_POSITION, ind_col.DESCEND, uc.CONSTRAINT_TYPE\n" +
			" FROM sys.ALL_INDEXES ind\n" +
			" LEFT JOIN sys.ALL_IND_COLUMNS ind_col ON ind.OWNER = ind_col.INDEX_OWNER AND ind.INDEX_NAME = ind_col.INDEX_NAME\n" +
			" LEFT JOIN sys.ALL_IND_EXPRESSIONS ind_col_ex on ind.OWNER = ind_col_ex.INDEX_OWNER AND ind.INDEX_NAME = ind_col_ex.INDEX_NAME\n" +
			" LEFT JOIN sys.ALL_CONSTRAINTS uc ON uc.INDEX_NAME = ind_col.INDEX_NAME AND ind.OWNER = uc.OWNER\n" +
			"WHERE ind.TABLE_OWNER = '%s' AND ind.TABLE_TYPE = 'TABLE'\n" +
			"  AND (uc.CONSTRAINT_TYPE IS NULL OR uc.CONSTRAINT_TYPE='U' OR uc.CONSTRAINT_TYPE='C')\n" +
			"  AND ind.TABLE_NAME IN (%s)\n" +
			"ORDER BY ind.TABLE_OWNER,ind.TABLE_NAME,ind.INDEX_NAME,ind_col.COLUMN_POSITION";
	private final static String ADD_INDEX_SQL = "CREATE %s INDEX \"%s\".\"%s\" ON \"%s\"(%s)";
	private final static String EXISTS_INDEX_SQL = "SELECT ind.INDEX_NAME from sys.ALL_INDEXES ind\n" +
			"WHERE ind.TABLE_OWNER = ? AND ind.TABLE_TYPE = 'TABLE'\n" +
			"  AND ind.TABLE_NAME = ? AND ind.INDEX_NAME = ?\n";

	@Override
	public void load(Connection conn, String schema, RelateDataBaseTable table) throws Exception {
		if (null == table) return;

		String indexName;
		String indexType;
		String collation;
		String nonUnique;
		String columnName;
		String columnExpression;
		int columnPosition;
		TableIndex tableIndex;
		TableIndexTypeEnums tableIndexType;
		Map<String, TableIndex> tableIndexMap = new LinkedHashMap<>();

		try (PreparedStatement ps = conn.prepareStatement(SELECT_INDEX_SQL)) {
			ps.setObject(1, schema);
			ps.setObject(2, table.getTable_name());
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					columnExpression = rs.getString("COLUMN_EXPRESSION");
					indexName = rs.getString("INDEX_NAME");
					indexType = rs.getString("INDEX_TYPE");
					collation = rs.getString("DESCEND");
					nonUnique = rs.getString("UNIQUENESS");
					columnName = rs.getString("COLUMN_NAME");
					columnPosition = rs.getInt("COLUMN_POSITION");

					if (null != columnExpression) {
						if (columnExpression.startsWith("\"")) {
							columnName = columnExpression.substring(1, columnExpression.length() - 1);
						} else {
							columnName = columnExpression;
							columnExpression = null;
						}
					}

					tableIndex = tableIndexMap.get(indexName);
					if (null == tableIndex) {
						tableIndexType = toIndexType(null == columnExpression ? indexType : "NORMAL");
						tableIndex = new TableIndex(indexName, null == tableIndexType ? null : tableIndexType.name(), indexType, "UNIQUE".equalsIgnoreCase(nonUnique), new ArrayList<>());
						tableIndexMap.put(indexName, tableIndex);
					}
					tableIndex.getColumns().add(new TableIndexColumn(columnName, columnPosition, (null == collation) ? null : "ASC".equalsIgnoreCase(collation)));
				}
			}
			table.setIndices(new ArrayList<>(tableIndexMap.values()));
		}
	}

	@Override
	public void load(Connection conn, String schema, List<RelateDataBaseTable> tables) throws Exception {
		if (CollectionUtils.isEmpty(tables)) {
			return;
		}
		StringBuilder tableNameClause = new StringBuilder();
		Map<String, RelateDataBaseTable> tableMap = new HashMap<>();
		String indexName;
		String indexType;
		String collation;
		String nonUnique;
		String columnName;
		String columnExpression;
		int columnPosition;
		String tableName;
		TableIndex tableIndex;
		TableIndexTypeEnums tableIndexType;
		Iterator<RelateDataBaseTable> iterator = tables.iterator();
		while (iterator.hasNext()) {
			RelateDataBaseTable table = iterator.next();
			tableNameClause.append("'").append(table.getTable_name()).append("'");
			if (iterator.hasNext()) {
				tableNameClause.append(",");
			}
			tableMap.put(table.getTable_name(), table);
		}
		String sql = String.format(SELECT_INDEX_SQL_TABLE_NAMES, schema, tableNameClause);
		try (
				Statement statement = conn.createStatement();
				ResultSet rs = statement.executeQuery(sql)
		) {
			while (rs.next()) {
				columnExpression = rs.getString("COLUMN_EXPRESSION");
				indexName = rs.getString("INDEX_NAME");
				indexType = rs.getString("INDEX_TYPE");
				collation = rs.getString("DESCEND");
				nonUnique = rs.getString("UNIQUENESS");
				columnName = rs.getString("COLUMN_NAME");
				columnPosition = rs.getInt("COLUMN_POSITION");
				tableName = rs.getString("TABLE_NAME");

				if (null != columnExpression) {
					if (columnExpression.startsWith("\"")) {
						columnName = columnExpression.substring(1, columnExpression.length() - 1);
					} else {
						columnName = columnExpression;
						columnExpression = null;
					}
				}

				List<TableIndex> indices = tableMap.get(tableName).getIndices();
				if (null != indices) {
					String finalIndexName = indexName;
					if (indices == null) {
						indices = new ArrayList<>();
					}
					tableIndex = indices.stream().filter(index -> index.getIndexName().equals(finalIndexName)).findFirst().orElse(null);
					if (null == tableIndex) {
						tableIndexType = toIndexType(null == columnExpression ? indexType : "NORMAL");
						tableIndex = new TableIndex(indexName, null == tableIndexType ? null : tableIndexType.name(), indexType, "UNIQUE".equalsIgnoreCase(nonUnique), new ArrayList<>());
						indices.add(tableIndex);
					}
					tableIndex.getColumns().add(new TableIndexColumn(columnName, columnPosition, (null == collation) ? null : "ASC".equalsIgnoreCase(collation)));
				}
			}
		}
	}

	@Override
	public void loadAll(Connection conn, String schema, Map<String, Map<String, TableIndex>> indexMap) throws Exception {
		throw new RuntimeException("Load all table index unrealized.");
	}

	@Override
	public void create(Connection conn, String schema, String tableName, TableIndex tableIndex) throws Exception {
		String uniqueStr = tableIndex.isUnique() ? " UNIQUE" : "";
		// 索引列
		TableIndexColumn tableIndexColumn;
		StringBuilder sql = new StringBuilder();
		List<TableIndexColumn> columns = tableIndex.getColumns();
		columns.sort(Comparator.comparingInt(TableIndexColumn::getColumnPosition));
		for (int i = 0, len = columns.size(); i < len; i++) {
			if (i > 0) {
				sql.append(",");
			}
			tableIndexColumn = columns.get(i);
			sql.append('"').append(tableIndexColumn.getColumnName()).append('"');
			if (null != tableIndexColumn.getColumnIsAsc()) {
				sql.append(tableIndexColumn.getColumnIsAsc() ? " ASC" : " DESC");
			}
		}
		String indexName = tableIndex.getIndexName();
		if (indexName.length() > 30) {
			if (!indexName.startsWith("KEY_")) {
				//做hash映射
				indexName = MD5Util.crypt(indexName, false);
			}
			indexName = indexName.substring(0, 30);
		}
		String createIndexSql = String.format(ADD_INDEX_SQL, uniqueStr, schema, indexName, tableName, sql);
		// 创建索引
		logger.info("Create index sql {}", createIndexSql);
		try (Statement s = conn.createStatement()) {
			s.execute(createIndexSql);
		}
	}

	@Override
	public boolean exist(Connection conn, String schema, String tableName, String indexName) throws Exception {
		try (PreparedStatement ps = conn.prepareStatement(EXISTS_INDEX_SQL)) {
			ps.setObject(1, schema);
			ps.setObject(2, tableName);
			ps.setObject(3, indexName);
			try (ResultSet rs = ps.executeQuery()) {
				return rs.next();
			}
		}
	}

	@Override
	public TableIndexTypeEnums toIndexType(String sourceIndexTypeStr) {
		if (null == sourceIndexTypeStr || sourceIndexTypeStr.isEmpty()) return null;

		sourceIndexTypeStr = sourceIndexTypeStr.toUpperCase();
		switch (sourceIndexTypeStr) {
			case "NORMAL":
				return TableIndexTypeEnums.BTREE;
			case "IOT - TOP":
			case "FUNCTION-BASED NORMAL":
			case "FUNCTION-BASED DOMAIN":
			default:
				return TableIndexTypeEnums.OTHER;
		}
	}

	@Override
	public String toSourceIndexType(TableIndexTypeEnums indexType) {
		if (null != indexType) {
			switch (indexType) {
				case BTREE:
					return null; // 只保留默认索引
				case RTREE:
				case TEXT:
				case HASH:
				case OTHER:
				default:
					return "";
			}
		}
		return null;
	}
}
