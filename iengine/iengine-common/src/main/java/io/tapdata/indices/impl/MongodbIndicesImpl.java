package io.tapdata.indices.impl;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.TableIndex;
import com.tapdata.entity.TableIndexColumn;
import com.tapdata.entity.TableIndexTypeEnums;
import io.tapdata.indices.IIndices;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 索引实现 - MongoDB
 * <pre>
 * Author: <a href="mailto:linhs@thoughtup.cn">Harsen</a>
 * CreateTime: 2021/4/14 下午1:17
 * </pre>
 */
public class MongodbIndicesImpl implements IIndices<MongoClient> {
	@Override
	public void load(MongoClient conn, String schema, RelateDataBaseTable table) throws Exception {
		if (null == table) return;

		MongoDatabase db = conn.getDatabase(schema);
		MongoCollection<Document> tableCollection = db.getCollection(table.getTable_name());

		String indexName;
		Boolean unique;
		TableIndex tableIndex;
		TableIndexTypeEnums tableIndexType;
		List<TableIndex> tableIndexList = new ArrayList<>();

		for (Document doc : tableCollection.listIndexes()) {
			indexName = doc.getString("name");
			unique = doc.getBoolean("unique", false);
			tableIndex = new TableIndex(indexName, null, null, unique, new ArrayList<>(), doc.toJson());

			int i = 0;
			Document key = doc.get("key", Document.class);
			for (String k : key.keySet()) {
				Object index = key.get(k);

				tableIndexType = toIndexType(tableIndex.getIndexSourceType());
				tableIndex.setIndexType(null == tableIndexType ? null : tableIndexType.name());


				tableIndex.getColumns().add(new TableIndexColumn(k, ++i, index));
			}
			tableIndexList.add(tableIndex);
		}
		table.setIndices(tableIndexList);
	}

	@Override
	public void loadAll(MongoClient conn, String schema, Map<String, Map<String, TableIndex>> indexMap) throws Exception {
		throw new RuntimeException("Load all table index unrealized.");
	}

	@Override
	public void create(MongoClient conn, String schema, String tableName, TableIndex tableIndex) throws Exception {
		MongoDatabase mongoDatabase = conn.getDatabase(schema);
		String usingIndexType = toSourceIndexType(tableIndex.getIndexType());
		if (null != usingIndexType && usingIndexType.isEmpty()) {
			return; // 不支持的索引类型，直接返回
		}

		Document indexDoc = new Document();
		for (TableIndexColumn tic : tableIndex.getColumns()) {
			if (null == usingIndexType) {
				if (null == tic.getColumnIsAsc() || !tic.getColumnIsAsc()) {
					indexDoc.put(tic.getColumnName(), -1);
				} else {
					indexDoc.put(tic.getColumnName(), 1);
				}
			} else {
				indexDoc.put(tic.getColumnName(), usingIndexType);
			}
		}
		IndexOptions indexOptions = new IndexOptions();
		indexOptions.unique(tableIndex.isUnique());
		indexOptions.name(tableIndex.getIndexName());

		MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(tableName);
		mongoCollection.createIndex(indexDoc, indexOptions);
	}

	@Override
	public boolean exist(MongoClient conn, String schema, String tableName, String indexName) throws Exception {
		MongoDatabase mongoDatabase = conn.getDatabase(schema);
		MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(tableName);
		for (Document doc : mongoCollection.listIndexes()) {
			if (indexName.equalsIgnoreCase(doc.getString("name"))) {
				return true;
			}
		}
		return false;
	}

	@Override
	public TableIndexTypeEnums toIndexType(String sourceIndexTypeStr) {
		if (null == sourceIndexTypeStr || sourceIndexTypeStr.isEmpty()) return null;

		sourceIndexTypeStr = sourceIndexTypeStr.toUpperCase();
		switch (sourceIndexTypeStr) {
			case "1":
			case "-1":
			case "1.0":
			case "-1.0":
				return TableIndexTypeEnums.BTREE; // 默认（1：升序;-1:降序）
			case "hashed":
				return TableIndexTypeEnums.HASH;
			case "text":
				return TableIndexTypeEnums.TEXT;
			case "2d": // 地理位置
			case "2dsphere": // 地理位置
			default:
				return TableIndexTypeEnums.OTHER;
		}
	}

	@Override
	public String toSourceIndexType(TableIndexTypeEnums indexType) {
		if (null != indexType) {
			switch (indexType) {
				case HASH:
					return "hashed";
				case TEXT:
					return "text";
				case BTREE:
					return null; // 默认
				case RTREE:
				case OTHER:
				default:
					return "";
			}
		}
		return null;
	}
}
