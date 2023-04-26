/**
 * @title: MongoDBScriptConnectionHandler
 * @description:
 * @author lk
 * @date 2020/8/6
 */
package com.tapdata.processor;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import io.tapdata.annotation.DatabaseTypeAnnotation;
import org.apache.commons.collections.map.HashedMap;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@DatabaseTypeAnnotation(type = DatabaseTypeEnum.MONGODB)
@DatabaseTypeAnnotation(type = DatabaseTypeEnum.ALIYUN_MONGODB)
public class MongoDBScriptConnectionHandler extends MongoDBScriptConnection {

	private MongoDatabase mongoDatabase;

	private MongoCollection<Document> mongoCollection;

	private MongoIterable<Document> mongoIterable;

	private MongoCursor<Document> mongoCursor;

	@Override
	public void initialize(Connections connections) {
		super.initialize(connections);
		this.db(defaultDatabaseName);
	}

	public ScriptConnection db(String db) {
		try {
			mongoDatabase = mongoClient.getDatabase(db);
		} catch (Exception e) {
			throw new RuntimeException(String.format("MongoDBScriptConnectionHandler error,method: db,message: %s", e.getMessage()));
		}
		return this;
	}

	public MongoDBScriptConnectionHandler collection(String collection) {
		try {
			if (mongoDatabase == null) {
				mongoDatabase = mongoClient.getDatabase(defaultDatabaseName);
			}

			mongoCollection = mongoDatabase.getCollection(collection);
		} catch (Exception e) {
			throw new RuntimeException(String.format("MongoDBScriptConnectionHandler error,method: collection,message: %s", e.getMessage()));
		}

		return this;
	}

	public MongoDBScriptConnectionHandler find() {
		return find(new HashedMap());
	}

	public MongoDBScriptConnectionHandler find(Map<String, Object> filter) {

		if (mongoCollection != null) {
			try {
				mongoIterable = mongoCollection.find(new Document(filter));
			} catch (Exception e) {
				throw new RuntimeException(String.format("MongoDBScriptConnectionHandler error,method: find,message: %s", e.getMessage()));
			}
		} else {
			throw new RuntimeException("mongoCollection cannot be null");
		}
		return this;
	}

	public long updateOne(Map<String, Object> filter, Map<String, Object> update) {
		return updateOne(filter, update, false);
	}

	public long updateOne(Map<String, Object> filter, Map<String, Object> update, boolean isUpsert) {
		long resultRows = 0;
		if (mongoCollection != null) {
			try {
				Document updateDoc = new Document();
				if (update.keySet().stream().anyMatch(u -> u.startsWith("$"))) {
					updateDoc.putAll(update);
				} else {
					updateDoc.append("$set", new Document(update));
				}
				UpdateResult updateResult = mongoCollection.updateOne(new Document(filter), updateDoc, new UpdateOptions().upsert(isUpsert));
				resultRows = updateResult.getModifiedCount();
			} catch (Exception e) {
				throw new RuntimeException(String.format("MongoDBScriptConnectionHandler error,method: updateOne,message: %s", e.getMessage()));
			}
		} else {
			throw new RuntimeException("mongoCollection cannot be null");
		}
		return resultRows;
	}

	public Map<String, Object> findAndModify(
			Map<String, Object> filter,
			Map<String, Object> update,
			boolean isUpsert,
			boolean returnNew
	) {
		Map<String, Object> resultRows = null;
		if (mongoCollection != null) {
			try {
				Document updateDoc = new Document();
				if (update.keySet().stream().anyMatch(u -> u.startsWith("$"))) {
					updateDoc.putAll(update);
				} else {
					updateDoc.append("$set", new Document(update));
				}
				resultRows = mongoCollection.findOneAndUpdate(
						new Document(filter),
						updateDoc,
						new FindOneAndUpdateOptions()
								.returnDocument(returnNew ? ReturnDocument.AFTER : ReturnDocument.BEFORE)
								.upsert(isUpsert)
				);
			} catch (Exception e) {
				throw new RuntimeException(String.format("MongoDBScriptConnectionHandler error,method: updateOne,message: %s", e.getMessage()));
			}
		} else {
			throw new RuntimeException("mongoCollection cannot be null");
		}
		return resultRows;
	}

	public long updateMany(Map<String, Object> filter, Map<String, Object> update) {
		return updateMany(filter, update, false);
	}

	public long updateMany(Map<String, Object> filter, Map<String, Object> update, boolean isUpsert) {
		long resultRows = 0;
		if (mongoCollection != null) {
			try {
				Document updateDoc = new Document();
				if (update.keySet().stream().anyMatch(u -> u.startsWith("$"))) {
					updateDoc.putAll(update);
				} else {
					updateDoc.append("$set", new Document(update));
				}
				UpdateResult updateResult = mongoCollection.updateMany(new Document(filter), updateDoc, new UpdateOptions().upsert(isUpsert));
				resultRows = updateResult.getModifiedCount();
			} catch (Exception e) {
				throw new RuntimeException(String.format("MongoDBScriptConnectionHandler error,method: updateMany,message: %s", e.getMessage()));
			}
		} else {
			throw new RuntimeException("mongoCollection cannot be null");
		}
		return resultRows;
	}

	public long insertOne(Map<String, Object> document) {
		long resultRows = 0;
		if (mongoCollection != null) {
			try {
				mongoCollection.insertOne(new Document(document));
				resultRows = 1;
			} catch (Exception e) {
				throw new RuntimeException(String.format("MongoDBScriptConnectionHandler error,method: insertOne,message: %s", e.getMessage()));
			}
		} else {
			throw new RuntimeException("mongoCollection cannot be null");
		}
		return resultRows;
	}

	public long insertMany(List<Map<String, Object>> documents) {
		long resultRows = 0;
		List<Document> list = new ArrayList<>();
		for (Map<String, Object> map : documents) {
			list.add(new Document(map));
		}
		if (mongoCollection != null) {
			try {
				mongoCollection.insertMany(list);
				resultRows = list.size();
			} catch (Exception e) {
				throw new RuntimeException(String.format("MongoDBScriptConnectionHandler error,method: insertMany,message: %s", e.getMessage()));
			}
		} else {
			throw new RuntimeException("mongoCollection cannot be null");
		}
		return resultRows;
	}

	public long deleteOne(Map<String, Object> filter) {
		long resultRows = 0;
		if (mongoCollection != null) {
			try {
				DeleteResult deleteResult = mongoCollection.deleteOne(new Document(filter));
				resultRows = deleteResult.getDeletedCount();
			} catch (Exception e) {
				throw new RuntimeException(String.format("MongoDBScriptConnectionHandler error,method: deleteOne,message: %s", e.getMessage()));
			}
		} else {
			throw new RuntimeException("mongoCollection cannot be null");
		}
		return resultRows;
	}

	public long deleteMany(Map<String, Object> filter) {
		long resultRows = 0;
		if (mongoCollection != null) {
			try {
				DeleteResult deleteResult = mongoCollection.deleteMany(new Document(filter));
				resultRows = deleteResult.getDeletedCount();
			} catch (Exception e) {
				throw new RuntimeException(String.format("MongoDBScriptConnectionHandler error,method: deleteMany,message: %s", e.getMessage()));
			}
		} else {
			throw new RuntimeException("mongoCollection cannot be null");
		}
		return resultRows;
	}

	public MongoDBScriptConnectionHandler aggregate(List<Map<String, Object>> pipeline) {
		return aggregate(pipeline, 30, 10 * 60);
	}

	public MongoDBScriptConnectionHandler aggregate(List<Map<String, Object>> pipeline, int maxAwaitTime, int maxTime) {
		if (pipeline == null) {
			pipeline = new ArrayList<>();
		}

		if (mongoCollection != null) {
			List<Document> documents = new ArrayList<>();
			pipeline.forEach(pipe -> {
				Document document = new Document();
				document.putAll(pipe);
				documents.add(document);
			});
			try {
				mongoIterable = mongoCollection.aggregate(documents).allowDiskUse(true)
						.maxAwaitTime(maxAwaitTime, TimeUnit.SECONDS).maxTime(maxTime, TimeUnit.SECONDS);
			} catch (Exception e) {
				logger.error("Execute mongodb aggregate failed, cause: " + e.getMessage() + ", pipeline: " + pipeline);
			}
		}
		return this;
	}

	public MongoDBScriptConnectionHandler filter(Map<String, Object> filter) {

		if (mongoIterable != null) {
			try {
				mongoIterable = ((FindIterable) mongoIterable).filter(new Document(filter));
			} catch (Exception e) {
				throw new RuntimeException(String.format("MongoDBScriptConnectionHandler error,method: filter,message: %s", e.getMessage()));
			}
		} else {
			throw new RuntimeException("mongoCollection cannot be null");
		}
		return this;
	}


	public MongoDBScriptConnectionHandler projection(Map<String, Object> projection) {

		if (mongoIterable != null) {
			try {
				mongoIterable = ((FindIterable) mongoIterable).projection(new Document(projection));
			} catch (Exception e) {
				throw new RuntimeException(String.format("MongoDBScriptConnectionHandler error,method: projection,message: %s", e.getMessage()));
			}
		} else {
			throw new RuntimeException("mongoCollection cannot be null");
		}
		return this;
	}


	public MongoDBScriptConnectionHandler limit(int limit) {

		if (mongoIterable != null) {
			try {
				mongoIterable = ((FindIterable) mongoIterable).limit(limit);
			} catch (Exception e) {
				throw new RuntimeException(String.format("MongoDBScriptConnectionHandler error,method: limit,message: %s", e.getMessage()));
			}
		} else {
			throw new RuntimeException("mongoCollection cannot be null");
		}
		return this;
	}

	public MongoDBScriptConnectionHandler skip(int skip) {

		if (mongoIterable != null) {
			try {
				mongoIterable = ((FindIterable) mongoIterable).skip(skip);
			} catch (Exception e) {
				throw new RuntimeException(String.format("MongoDBScriptConnectionHandler error,method: skip,message: %s", e.getMessage()));
			}
		} else {
			throw new RuntimeException("mongoCollection cannot be null");
		}
		return this;
	}

	public MongoDBScriptConnectionHandler batchSize(int batchSize) {

		if (mongoIterable != null) {
			try {
				mongoIterable = mongoIterable.batchSize(batchSize);
			} catch (Exception e) {
				throw new RuntimeException(String.format("MongoDBScriptConnectionHandler error,method: batchSize,message: %s", e.getMessage()));
			}
		} else {
			throw new RuntimeException("mongoCollection cannot be null");
		}
		return this;
	}

	public MongoDBScriptConnectionHandler sort(Map<String, Object> sort) {

		if (mongoIterable != null) {
			try {
				mongoIterable = ((FindIterable) mongoIterable).sort(new Document(sort));
			} catch (Exception e) {
				throw new RuntimeException(String.format("MongoDBScriptConnectionHandler error,method: sort,message: %s", e.getMessage()));
			}
		} else {
			throw new RuntimeException("mongoCollection cannot be null");
		}
		return this;
	}

	public List<Map<String, Object>> toArray() {
		List<Map<String, Object>> resultList = new ArrayList<>();
		if (mongoIterable != null) {
			try (MongoCursor<Document> mongoCursor = mongoIterable.iterator()) {
				while (mongoCursor.hasNext()) {
					resultList.add(mongoCursor.next());
				}
			} catch (Exception e) {
				throw new RuntimeException(String.format("MongoDBScriptConnectionHandler error,method: toArray,message: %s", e.getMessage()));
			}
		} else {
			throw new RuntimeException("mongoCollection cannot be null");
		}
		return resultList;
	}

	public boolean hasNext() {
		if (mongoIterable == null) {
			return false;
		}
		if (mongoCursor == null) {
			mongoCursor = mongoIterable.iterator();
		}
		return mongoCursor.hasNext();
	}

	public Map<String, Object> next() {
		if (mongoIterable == null) {
			return null;
		}
		if (mongoCursor == null) {
			mongoCursor = mongoIterable.iterator();
		}
		if (mongoCursor.hasNext()) {
			return mongoCursor.next();
		} else {
			return null;
		}
	}

	public MongoCursor<Document> cursor() {
		MongoCursor<Document> cursor = null;
		if (mongoIterable != null) {
			cursor = mongoIterable.iterator();
		} else {
			throw new RuntimeException("mongoCollection cannot be null");
		}

		return cursor;
	}

	public boolean tableExists(String tableName) {
		if (mongoDatabase == null) {
			mongoDatabase = mongoClient.getDatabase(defaultDatabaseName);
		}

		try (MongoCursor<String> cursor = mongoDatabase.listCollectionNames().iterator()) {
			while (cursor.hasNext()) {
				String collectionName = cursor.next();
				if (collectionName.equals(tableName)) {
					return true;
				}
			}
		}

		return false;
	}

	@Override
	public String createIndex(Map<String, Object> index) {
		String indexName = null;
		if (mongoCollection != null) {
			indexName = mongoCollection.createIndex(
					new Document(index),
					new IndexOptions().background(true)
			);
		} else {
			throw new RuntimeException("mongoCollection cannot be null");
		}
		return indexName;
	}

	@Override
	public void drop() {
		if (mongoCollection != null) {
			mongoCollection.drop();
		} else {
			throw new RuntimeException("mongoCollection cannot be null");
		}
	}
}
