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
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JDBC索引实现 - PostgreSQL
 * <pre>
 * Author: <a href="mailto:linhs@thoughtup.cn">Harsen</a>
 * CreateTime: 2021/4/14 下午1:02
 * </pre>
 */
public class PostgresqlIndicesImpl implements IIndices<Connection> {
	private final static Logger logger = LogManager.getLogger(PostgresqlIndicesImpl.class);

	private final static Pattern PATTERN = Pattern.compile("CREATE (UNIQUE )?INDEX ([^ ]+) ON [^.]+\\.[^ ]+ (USING ([^ ]+))? \\(([^)]+)\\)");
	private final static Pattern PATTERN_FIELDS = Pattern.compile("([^ ),]+)( ASC| DESC)?");
	private final static String SELECT_INDEX_SQL = "select pg_get_indexdef(pi.indexrelid) AS INDEXDEF from pg_index pi\n" +
			"  left join pg_class pc on pi.indexrelid = pc.oid\n" +
			"  left join pg_class pct on pi.indrelid = pct.oid\n" +
			"  left join pg_namespace pn on pc.relnamespace = pn.oid\n" +
			" where pi.indisprimary = 'f' and pn.nspname = ? and pct.relname = ?";
	private final static String ADD_INDEX_SQL = "CREATE%s INDEX \"%s\" ON \"%s\".\"%s\"%s(%s)";
	private final static String EXISTS_INDEX_SQL = "select INDEXNAME from PG_INDEXES pi where pi.SCHEMANAME = ? and pi.TABLENAME = ? and INDEXNAME = ?";

	@Override
	public void load(Connection conn, String schema, RelateDataBaseTable table) throws Exception {
		if (null == table) return;

		String indexDef;
		TableIndex tableIndex;
		List<TableIndex> tableIndices = new ArrayList<>();

		try (PreparedStatement ps = conn.prepareStatement(SELECT_INDEX_SQL)) {
			ps.setObject(1, schema);
			ps.setObject(2, table.getTable_name());
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					indexDef = rs.getString("INDEXDEF");
					// 解析索引
					tableIndex = definedSql2TableIndex(indexDef);
					if (null != tableIndex) {
						tableIndices.add(tableIndex);
					}
				}
			}
			if (!tableIndices.isEmpty()) {
				table.setIndices(tableIndices);
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
			sql.append("\"").append(tableIndexColumn.getColumnName()).append("\"");
			if (null != tableIndexColumn.getColumnIsAsc()) {
				sql.append(tableIndexColumn.getColumnIsAsc() ? " ASC" : " DESC");
			}
		}
		String createIndexSql = String.format(ADD_INDEX_SQL, uniqueStr, tableIndex.getIndexName(), schema, tableName, usingIndexType, sql.toString());
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
			case "HASH":
				return TableIndexTypeEnums.HASH;
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
				case HASH:
					return "HASH";
				default:
					logger.warn("TableIndexType creator unrealized '{}'.", indexType);
					return "";
			}
		}
		return null;
	}

	/**
	 * DDL转索引
	 *
	 * @param definedSql 定义SQL
	 * @return 索引信息
	 */
	private TableIndex definedSql2TableIndex(String definedSql) {
		Matcher m = PATTERN.matcher(definedSql);
		if (m.find()) {
			TableIndexTypeEnums tableIndexType;
			TableIndex tableIndex = new TableIndex();

			tableIndex.setUnique(null != m.group(1));
			tableIndex.setIndexName(unquote(m.group(2)));
			tableIndex.setIndexSourceType(m.group(4));
			tableIndexType = toIndexType(tableIndex.getIndexSourceType());
			if (null != tableIndexType) {
				tableIndex.setIndexType(tableIndexType.name());
			}

			int i = 1;
			tableIndex.setColumns(new ArrayList<>());
			Matcher mf = PATTERN_FIELDS.matcher(m.group(5));
			while (mf.find()) {
				tableIndex.getColumns().add(new TableIndexColumn(unquote(mf.group(1)), i++, null == m.group(2) ? null : " ASC".equalsIgnoreCase(m.group(2))));
			}
			return tableIndex;
		}
		return null;
	}
}
