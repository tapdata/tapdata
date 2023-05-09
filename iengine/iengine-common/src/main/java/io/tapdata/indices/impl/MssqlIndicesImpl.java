package io.tapdata.indices.impl;

import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.TableIndex;
import com.tapdata.entity.TableIndexColumn;
import com.tapdata.entity.TableIndexTypeEnums;
import io.tapdata.indices.IIndices;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC索引实现 - SQLServer|MsSQL
 * <pre>
 * Author: <a href="mailto:linhs@thoughtup.cn">Harsen</a>
 * CreateTime: 2021/4/14 下午12:33
 * </pre>
 */
public class MssqlIndicesImpl implements IIndices<Connection> {
	private final static Logger logger = LogManager.getLogger(MssqlIndicesImpl.class);

	private final static String SELECT_INDEX_SQL = "SELECT t_i.Name IndexName, t_i.type_desc IndexType, t_i.is_primary_key IsPrimaryKey, t_i.is_unique IndexUnique, t_c.Name ColumnName, t_ic.index_column_id ColumnPosition,\n" +
			"  CASE INDEXKEY_PROPERTY(t_ic.object_id, t_ic.index_id, t_ic.index_column_id, 'IsDescending') WHEN 1 THEN 'DESC' WHEN 0 THEN 'ASC' ELSE null END ColumnCollation\n" +
			"  FROM sys.indexes t_i\n" +
			" INNER JOIN sys.index_columns t_ic ON t_i.object_id = t_ic.object_id AND t_i.index_id= t_ic.index_id\n" +
			" INNER JOIN sys.columns t_c ON t_c.object_id = t_ic.object_id AND t_c.column_id= t_ic.Column_id\n" +
			" WHERE t_i.is_disabled = 0 and object_schema_name(t_i.object_id) = ? and object_name(t_i.object_id) = ?\n" +
			" ORDER BY t_i.index_id, t_ic.index_column_id"; // 需要过滤外键

	private final static String EXISTS_INDEX_SQL = "select ti.name from sys.indexes ti\n" +
			"where object_schema_name(ti.object_id) = ? and object_name(ti.object_id) = ? and ti.name = ?";

	@Override
	public void load(Connection conn, String schema, RelateDataBaseTable table) throws Exception {
		if (null == table) return;

		String indexName;
		String indexType;
		String collation;
		Boolean isUnique;
		Boolean isPrimaryKey;
		String columnName;
		int columnPosition;
		TableIndex tableIndex;
		TableIndexTypeEnums tableIndexType;
		Map<String, TableIndex> tableIndexMap = new LinkedHashMap<>();

		try (PreparedStatement ps = conn.prepareStatement(SELECT_INDEX_SQL)) {
			ps.setObject(1, schema);
			ps.setObject(2, table.getTable_name());
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					indexName = rs.getString("IndexName");
					indexType = rs.getString("IndexType");
					isPrimaryKey = rs.getBoolean("IsPrimaryKey");
					collation = rs.getString("ColumnCollation");
					isUnique = rs.getBoolean("IndexUnique");
					columnName = rs.getString("ColumnName");
					columnPosition = rs.getInt("ColumnPosition");
					tableIndex = tableIndexMap.get(indexName);
					if (null == tableIndex) {
						tableIndexType = toIndexType(indexType);
						tableIndex = new TableIndex(indexName, null == tableIndexType ? null : tableIndexType.name(), indexType, isUnique, new ArrayList<>());
						tableIndex.setClustered("CLUSTERED".equalsIgnoreCase(indexType));
						tableIndex.setPrimaryKey(isPrimaryKey);
						tableIndexMap.put(indexName, tableIndex);
					}
					tableIndex.getColumns().add(new TableIndexColumn(columnName, columnPosition, (null == collation) ? null : "ASC".equalsIgnoreCase(collation)));
				}
			}
			table.setIndices(new ArrayList<>(tableIndexMap.values()));
		}
	}

	@Override
	public void loadAll(Connection conn, String schema, Map<String, Map<String, TableIndex>> indexMap) throws Exception {
		throw new RuntimeException("Load all table index unrealized.");
	}

	@Override
	public void create(Connection conn, String schema, String tableName, TableIndex tableIndex) throws Exception {
		StringBuilder sql = new StringBuilder();
		if (tableIndex.isPrimaryKey()) {
			sql.append("ALTER TABLE")
					.append(" [").append(schema).append("].[").append(tableName).append("]")
					.append(" ADD PRIMARY KEY").append(tableIndex.isClustered() ? "" : " NONCLUSTERED").append(" (");
		} else {
			sql.append("CREATE").append(tableIndex.isUnique() ? " UNIQUE" : "");
			// 索引类型
			String usingIndexType = toSourceIndexType(tableIndex.getIndexType());
			if (null == usingIndexType) {
				sql.append(" NONCLUSTERED");
			} else if (usingIndexType.isEmpty()) {
				return; // 不支持的索引类型，直接返回
			} else {
				if (tableIndex.isClustered()) {
					sql.append(" CLUSTERED");
				} else {
					sql.append(" NONCLUSTERED");
				}
			}
			sql.append(" INDEX")
					.append(" [").append(tableIndex.getIndexName()).append("]")
					.append(" ON [").append(schema).append("]").append(".[").append(tableName).append("] (");
		}

		// 索引列
		TableIndexColumn tableIndexColumn;
		List<TableIndexColumn> columns = tableIndex.getColumns();
		columns.sort(Comparator.comparingInt(TableIndexColumn::getColumnPosition));
		for (int i = 0, len = columns.size(); i < len; i++) {
			if (i > 0) {
				sql.append(",");
			}
			tableIndexColumn = columns.get(i);
			sql.append("[").append(tableIndexColumn.getColumnName()).append("]");

			// 排序设置
			if (null != tableIndexColumn.getColumnIsAsc()) {
				sql.append(tableIndexColumn.getColumnIsAsc() ? " ASC" : " DESC");
			}
		}
		sql.append(")");

		// 创建索引
		String createIndexSql = sql.toString();
		logger.info("Create index sql {}", createIndexSql);
		try (Statement s = conn.createStatement()) {
			s.execute(createIndexSql);
		}

		// 如果主键是聚集索引，需要创建一个非主键的非聚集索引
		// 不然全量任务，重置后再次启动，会出现异常：Transaction (Process ID 73) was deadlocked on lock resources with another process and has been chosen as the deadlock victim. Rerun the transaction
		// todo: 需要定位死锁原因 20211221
		if (tableIndex.isPrimaryKey() && tableIndex.isClustered()) {
			TableIndex ti = (TableIndex) tableIndex.clone();
			ti.setPrimaryKey(false);
			ti.setClustered(false);
			create(conn, schema, tableName, ti);
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
			case "CLUSTERED":
			case "NONCLUSTERED":
				return TableIndexTypeEnums.BTREE;
			default:
				return TableIndexTypeEnums.OTHER;
		}
	}

	@Override
	public String toSourceIndexType(TableIndexTypeEnums indexType) {
		if (null != indexType) {
			switch (indexType) {
				case BTREE:
					return "BTREE";
				default:
					return "";
			}
		}
		return null;
	}
}
