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
 * Jdbc表索引实现 - MySQL
 * <pre>
 * Author: <a href="mailto:linhs@thoughtup.cn">Harsen</a>
 * CreateTime: 2021/4/14 下午12:31
 * </pre>
 */
public class MysqlIndicesImpl implements IIndices<Connection> {
	private final static Logger logger = LogManager.getLogger(MysqlIndicesImpl.class);

	private final static String SELECT_INDEX_SQL = "select i.INDEX_NAME, i.INDEX_TYPE, i.COLLATION, i.NON_UNIQUE, i.COLUMN_NAME, i.SEQ_IN_INDEX from INFORMATION_SCHEMA.STATISTICS i\n" +
			"  left join INFORMATION_SCHEMA.KEY_COLUMN_USAGE k\n" +
			"    on i.TABLE_SCHEMA = k.TABLE_SCHEMA\n" +
			"   and i.TABLE_NAME = k.TABLE_NAME\n" +
			"   and i.INDEX_NAME = CONCAT(k.CONSTRAINT_NAME,'_idx')\n" +
			"   and i.COLUMN_NAME = k.COLUMN_NAME\n" +
			" where i.TABLE_SCHEMA = ?\n" +
			"   and i.TABLE_NAME = ?\n" +
			"   and i.INDEX_NAME <> 'PRIMARY'\n" +
			"   and k.CONSTRAINT_NAME is null"; // 需要过滤主键、外键
	private final static String SELECT_ALL_INDEX_SQL = "select i.TABLE_NAME, i.INDEX_NAME, i.INDEX_TYPE, i.COLLATION, i.NON_UNIQUE, i.COLUMN_NAME, i.SEQ_IN_INDEX from INFORMATION_SCHEMA.STATISTICS i\n" +
			"  left join INFORMATION_SCHEMA.KEY_COLUMN_USAGE k\n" +
			"    on i.TABLE_SCHEMA = k.TABLE_SCHEMA\n" +
			"   and i.TABLE_NAME = k.TABLE_NAME\n" +
			"   and i.INDEX_NAME = CONCAT(k.CONSTRAINT_NAME,'_idx')\n" +
			"   and i.COLUMN_NAME = k.COLUMN_NAME\n" +
			" where i.TABLE_SCHEMA = ?\n" +
			"   and i.INDEX_NAME <> 'PRIMARY'\n" +
			"   and k.CONSTRAINT_NAME is null"; // 需要过滤主键、外键
	private final static String ADD_INDEX_SQL = "ALTER TABLE `%s`.`%s` ADD%s INDEX `%s`%s(%s)";
	private final static String EXISTS_INDEX_SQL = "select i.INDEX_NAME from INFORMATION_SCHEMA.STATISTICS i where i.TABLE_SCHEMA = ? and i.TABLE_NAME = ? and i.INDEX_NAME = ?";

	@Override
	public void load(Connection conn, String schema, RelateDataBaseTable table) throws Exception {
		if (null == table) return;

		String indexName;
		String indexType;
		String collation;
		Boolean nonUnique;
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
					indexName = rs.getString("INDEX_NAME");
					indexType = rs.getString("INDEX_TYPE");
					collation = rs.getString("COLLATION");
					nonUnique = rs.getBoolean("NON_UNIQUE");
					columnName = rs.getString("COLUMN_NAME");
					columnPosition = rs.getInt("SEQ_IN_INDEX");
					tableIndex = tableIndexMap.get(indexName);
					if (null == tableIndex) {
						tableIndexType = toIndexType(indexType);
						tableIndex = new TableIndex(indexName, null == tableIndexType ? null : tableIndexType.name(), indexType, !nonUnique, new ArrayList<>());
						tableIndexMap.put(indexName, tableIndex);
					}
					tableIndex.getColumns().add(new TableIndexColumn(columnName, columnPosition, (null == collation) ? null : "A".equalsIgnoreCase(collation)));
				}
			}
			table.setIndices(new ArrayList<>(tableIndexMap.values()));
		}
	}

	@Override
	public void loadAll(Connection conn, String schema, Map<String, Map<String, TableIndex>> indexMap) throws Exception {
		String tableName;
		String indexName;
		String indexType;
		String collation;
		Boolean nonUnique;
		String columnName;
		int columnPosition;
		TableIndex tableIndex;
		TableIndexTypeEnums tableIndexType;
		Map<String, TableIndex> tableIndexMap;

		try (PreparedStatement ps = conn.prepareStatement(SELECT_ALL_INDEX_SQL)) {
			ps.setObject(1, schema);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					tableName = rs.getString("TABLE_NAME");
					indexName = rs.getString("INDEX_NAME");
					indexType = rs.getString("INDEX_TYPE");
					collation = rs.getString("COLLATION");
					nonUnique = rs.getBoolean("NON_UNIQUE");
					columnName = rs.getString("COLUMN_NAME");
					columnPosition = rs.getInt("SEQ_IN_INDEX");
					tableIndexMap = indexMap.computeIfAbsent(schema + "." + tableName, k -> new LinkedHashMap<>());
					tableIndex = tableIndexMap.get(indexName);
					if (null == tableIndex) {
						tableIndexType = toIndexType(indexType);
						tableIndex = new TableIndex(indexName, null == tableIndexType ? null : tableIndexType.name(), indexType, !nonUnique, new ArrayList<>());
						tableIndexMap.put(indexName, tableIndex);
					}
					tableIndex.getColumns().add(new TableIndexColumn(columnName, columnPosition, (null == collation) ? null : "A".equalsIgnoreCase(collation)));
				}
			}
		}
	}

	@Override
	public void create(Connection conn, String schema, String tableName, TableIndex tableIndex) throws Exception {
		String uniqueStr = tableIndex.isUnique() ? " UNIQUE" : "";
		// 索引类型
		String usingIndexType = toSourceIndexType(tableIndex.getIndexType());
		if (null == usingIndexType) {
			usingIndexType = "";
		} else if (usingIndexType.isEmpty()) {
			return; // 不支持的索引类型，直接返回
		} else {
			usingIndexType = " USING " + usingIndexType;
		}
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
			sql.append("`").append(tableIndexColumn.getColumnName()).append("`");

			// 排序设置
			if (null != tableIndexColumn.getColumnIsAsc()) {
				sql.append(tableIndexColumn.getColumnIsAsc() ? " ASC" : " DESC");
			}
		}
		String createIndexSql = String.format(ADD_INDEX_SQL, schema, tableName, uniqueStr, tableIndex.getIndexName(), usingIndexType, sql.toString());
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
			case "BTREE":
				return TableIndexTypeEnums.BTREE;
			case "FULLTEXT":
				return TableIndexTypeEnums.TEXT;
			case "HASH":
				return TableIndexTypeEnums.HASH;
			case "RTREE":
				return TableIndexTypeEnums.RTREE;
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
				case RTREE:
					return "RTREE";
				case TEXT:
					return "TEXT";
				case HASH:
					return "HASH";
				default:
					return "";
			}
		}
		return null;
	}
}
