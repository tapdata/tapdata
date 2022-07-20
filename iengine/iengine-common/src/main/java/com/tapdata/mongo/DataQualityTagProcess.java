package com.tapdata.mongo;

import com.tapdata.entity.DataQualityTag;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.ProcessResult;
import org.apache.commons.collections.CollectionUtils;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.List;

public class DataQualityTagProcess {

	public static final String UPDATE_RESULT_FIELD = new StringBuilder(DataQualityTag.SUB_COLUMN_NAME).append(".").append(DataQualityTag.RESULT_FIELD).toString();

	public static final String UPDATE_HITRULES_FIELD = new StringBuilder(DataQualityTag.SUB_COLUMN_NAME).append(".").append(DataQualityTag.HITRULES_FIELD).toString();

	public static ProcessResult dataQualityTagHandle(ProcessResult processResult) {
		DataQualityTag dataQualityTag = processResult.getDataQualityTag();
		Update update = processResult.getUpdate();
		if (update != null && processResult != null) {

			if (dataQualityTag != null && CollectionUtils.isNotEmpty(dataQualityTag.getHitRules())) {
				List<DataQualityTag.HitRules> hitRules = dataQualityTag.getHitRules();
				update.set(UPDATE_RESULT_FIELD, dataQualityTag.getResult());
				update.addToSet(UPDATE_HITRULES_FIELD, new Document("$each", hitRules));
			} else {
				update.unset(UPDATE_RESULT_FIELD);
				update.unset(UPDATE_HITRULES_FIELD);
			}
		}


		return processResult;
	}

	public static ProcessResult mergeDataQualityTagToProcessResult(DataQualityTag tag, ProcessResult processResult) {

		if (tag != null) {
			DataQualityTag dataQualityTag = processResult.getDataQualityTag();
			if (dataQualityTag == null) {
				processResult.setDataQualityTag(tag);
				return processResult;
			}

			if (CollectionUtils.isNotEmpty(tag.getHitRules())) {
				List<DataQualityTag.HitRules> hitRules = dataQualityTag.getHitRules();
				if (hitRules == null) {
					hitRules = new ArrayList<>();
					dataQualityTag.setHitRules(hitRules);
				}

				hitRules.addAll(tag.getHitRules());
			}

		}

		return processResult;
	}

	public static void handleDataQualityTag(MessageEntity msg) {
		if (msg == null) {
			return;
		}
		DataQualityTag dataQualityTag = msg.getDataQualityTag();
		if (dataQualityTag == null) {
			dataQualityTag = new DataQualityTag(DataQualityTag.INVALID_RESULT);
			msg.setDataQualityTag(dataQualityTag);
		}
		List<DataQualityTag.HitRules> hitRules = dataQualityTag.getHitRules();
		if (hitRules == null) {
			hitRules = new ArrayList<>();
			dataQualityTag.setHitRules(hitRules);
		}
		List<DataQualityTag.HitRules> passRules = dataQualityTag.getPassRules();
		if (passRules != null) {
			passRules = new ArrayList<>();
			dataQualityTag.setPassRules(passRules);
		}
	}
}
