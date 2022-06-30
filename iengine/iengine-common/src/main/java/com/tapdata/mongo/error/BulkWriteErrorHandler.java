package com.tapdata.mongo.error;

import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

/**
 * @author samuel
 * @Description
 * @create 2021-08-25 16:19
 **/
public interface BulkWriteErrorHandler {
	boolean handle(WriteModel<Document> writeModel, BulkWriteError bulkWriteError);
}
