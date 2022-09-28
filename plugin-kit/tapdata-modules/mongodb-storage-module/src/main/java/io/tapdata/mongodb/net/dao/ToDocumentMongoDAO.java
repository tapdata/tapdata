package io.tapdata.mongodb.net.dao;

import com.mongodb.MongoWriteException;
import com.mongodb.client.result.UpdateResult;
import io.tapdata.mongodb.dao.AbstractMongoDAO;
import io.tapdata.mongodb.entity.ToDocument;
import org.bson.Document;

public abstract class ToDocumentMongoDAO<T extends ToDocument> extends AbstractMongoDAO<T> {
	@Override
	public boolean insertOne(T t) {
		boolean bool = false;
		try {
			bool = super.insertOne(t);
		} catch (MongoWriteException e) {
			if(e.getError().getCode() == 11000) {
				UpdateResult result = updateOne(new Document("_id", t.getId()), new Document("$set", t.toDocument(new Document())), true);
				if(result != null) {
					bool = result.getModifiedCount() > 0;
				}
			}
		}
		return bool;
	}
}
