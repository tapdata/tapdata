package io.tapdata.mongodb.writer;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2023-04-23 18:34
 **/
public class BulkWriteModel {
	private boolean allInsert = false;
	// 如果有update/delete事件，则使用这个write model list
	// 或者写入遇到了重复值错误，则使用这个write model list
	private List<WriteModel<Document>> allOpWriteModels;
	// 如果都是写入事件，则使用这个write model list，会有更好的性能
	private List<WriteModel<Document>> onlyInsertWriteModels;

	public BulkWriteModel(boolean allInsert) {
		this.allInsert = allInsert;
	}

	public boolean isAllInsert() {
		return allInsert;
	}

	public boolean getAllInsert() {
		return allInsert;
	}

	public List<WriteModel<Document>> getAllOpWriteModels() {
		return allOpWriteModels;
	}

	public List<WriteModel<Document>> getOnlyInsertWriteModels() {
		return onlyInsertWriteModels;
	}

	public void addAnyOpModel(WriteModel<Document> writeModel) {
		if (null == allOpWriteModels) {
			allOpWriteModels = new ArrayList<>();
		}
		allOpWriteModels.add(writeModel);
	}

	public void addOnlyInsertModel(InsertOneModel<Document> insertOneModel) {
		if (null == onlyInsertWriteModels) {
			onlyInsertWriteModels = new ArrayList<>();
		}
		onlyInsertWriteModels.add(insertOneModel);
	}

	public void setAllInsert(boolean allInsert) {
		this.allInsert = allInsert;
		if (null != onlyInsertWriteModels) {
			onlyInsertWriteModels.clear();
		}
	}

	public boolean isEmpty() {
		return CollectionUtils.isEmpty(allOpWriteModels) && CollectionUtils.isEmpty(onlyInsertWriteModels);
	}

	public List<WriteModel<Document>> getWriteModels() {
		if (allInsert) {
			return CollectionUtils.isNotEmpty(onlyInsertWriteModels) ? onlyInsertWriteModels : allOpWriteModels;
		} else {
			return allOpWriteModels;
		}
	}

	public void clearAll() {
		if (null != allOpWriteModels) {
			allOpWriteModels.clear();
		}
		if (null != onlyInsertWriteModels) {
			onlyInsertWriteModels.clear();
		}
	}
}
