package io.tapdata.mongodb.writer.error.handler;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import io.tapdata.mongodb.writer.BulkWriteModel;
import io.tapdata.mongodb.writer.error.BulkWriteErrorHandler;
import org.bson.Document;

/**
 * @author samuel
 * @Description Duplicate key error
 * @create 2023-04-23 19:10
 **/
public class Code11000Handler implements BulkWriteErrorHandler {
	@Override
	public WriteModel<Document> handle(
			BulkWriteModel bulkWriteModel,
			WriteModel<Document> writeModel,
			BulkWriteOptions bulkWriteOptions,
			MongoBulkWriteException mongoBulkWriteException,
			BulkWriteError writeError,
			MongoCollection<Document> collection
	) {
		bulkWriteModel.setAllInsert(false);
		return writeModel;
	}
}
