package io.tapdata.mongodb.writer.error.handler;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import io.tapdata.mongodb.writer.BulkWriteModel;
import io.tapdata.mongodb.writer.error.BulkWriteErrorHandler;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;

import java.util.List;

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
		if (bulkWriteModel.isAllInsert()) {
			int index = writeError.getIndex();
			List<WriteModel<Document>> allOpWriteModels = bulkWriteModel.getAllOpWriteModels();
			if (CollectionUtils.isEmpty(allOpWriteModels)) {
				return null;
			}
			try {
				return allOpWriteModels.get(index);
			} catch (Exception ignored) {
				return null;
			}
		} else {
			return null;
		}
	}
}
