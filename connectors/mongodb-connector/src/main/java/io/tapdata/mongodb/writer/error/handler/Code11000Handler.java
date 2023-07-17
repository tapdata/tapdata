package io.tapdata.mongodb.writer.error.handler;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.WriteModel;
import io.tapdata.entity.logger.TapLogger;
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
	private static final String TAG = Code11000Handler.class.getSimpleName();

	@Override
	public WriteModel<Document> handle(
			BulkWriteModel bulkWriteModel,
			WriteModel<Document> writeModel,
			BulkWriteOptions bulkWriteOptions,
			MongoBulkWriteException mongoBulkWriteException,
			BulkWriteError writeError,
			MongoCollection<Document> collection
	) {
		if (bulkWriteModel.isAllInsert() || isContainDocument(bulkWriteModel, writeError)) {
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

	private boolean isContainDocument(BulkWriteModel bulkWriteModel, BulkWriteError writeError) {
		if (CollectionUtils.isEmpty(bulkWriteModel.getOnlyInsertWriteModels())) {
			return false;
		}
		try {
			WriteModel<Document> errorWriteModel;
			int index = writeError.getIndex();
			List<WriteModel<Document>> allOpWriteModels = bulkWriteModel.getAllOpWriteModels();
			if (CollectionUtils.isEmpty(allOpWriteModels)) {
				return false;
			}
			try {
				errorWriteModel = allOpWriteModels.get(index);
				if (errorWriteModel == null) {
					return false;
				}
			} catch (Exception ignored) {
				return false;
			}
			for (WriteModel<Document> writeModel : bulkWriteModel.getOnlyInsertWriteModels()) {
				if (errorWriteModel instanceof UpdateManyModel) {
					String id = ((Document) ((UpdateManyModel) errorWriteModel).getFilter()).get("_id").toString();
					String idTemp = ((Document) ((InsertOneModel) writeModel).getDocument()).get("_id").toString();
					if (id.equals(idTemp)) {
						return true;
					}
				}
			}
		} catch (Exception e) {
			TapLogger.error(TAG, "Code11000Handler handle containDocument error", e);
			return false;
		}
		return false;
	}
}
