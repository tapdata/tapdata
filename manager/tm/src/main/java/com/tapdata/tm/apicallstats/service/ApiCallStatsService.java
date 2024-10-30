package com.tapdata.tm.apicallstats.service;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.tapdata.tm.apicallstats.dto.ApiCallStatsDto;
import com.tapdata.tm.apicallstats.entity.ApiCallStatsEntity;
import com.tapdata.tm.apicallstats.repository.ApiCallStatsRepository;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.utils.DocumentUtils;
import com.tapdata.tm.utils.EntityUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @Author: sam
 * @Date: 2024/08/26
 * @Description: ApiCallStatsService
 */
@Service
@Slf4j
public class ApiCallStatsService extends BaseService<ApiCallStatsDto, ApiCallStatsEntity, ObjectId, ApiCallStatsRepository> {

	public ApiCallStatsService(@NonNull ApiCallStatsRepository repository) {
		super(repository, ApiCallStatsDto.class, ApiCallStatsEntity.class);
	}

	protected void beforeSave(ApiCallStatsDto apiCallStats, UserDetail user) {

	}

	public void merge(ApiCallStatsDto oldStats, ApiCallStatsDto newStats) {
		if (null == oldStats && null == newStats) {
			return;
		}
		if (null != oldStats) {
			newStats.setId(oldStats.getId());
			newStats.setCallTotalCount(newStats.getCallTotalCount() + oldStats.getCallTotalCount());
			newStats.setTransferDataTotalBytes(newStats.getTransferDataTotalBytes() + oldStats.getTransferDataTotalBytes());
			newStats.setCallAlarmTotalCount(newStats.getCallAlarmTotalCount() + oldStats.getCallAlarmTotalCount());
			newStats.setResponseDataRowTotalCount(newStats.getResponseDataRowTotalCount() + oldStats.getResponseDataRowTotalCount());
			newStats.setTotalResponseTime(newStats.getTotalResponseTime() + oldStats.getTotalResponseTime());
			if (CollectionUtils.isNotEmpty(oldStats.getClientIds())) {
				newStats.getClientIds().addAll(oldStats.getClientIds());
			}
			newStats.setCreateAt(oldStats.getCreateAt());
			if (log.isDebugEnabled()) {
				log.debug("ApiCallStatsService.merge oldStats: {}, newStats: {}", oldStats, newStats);
			}
		} else {
			newStats.setId(new ObjectId());
			newStats.setCreateAt(new Date());
		}
	}

	public ApiCallStatsDto aggregateByUserId(String userId) {
		ApiCallStatsDto apiCallStatsDto = new ApiCallStatsDto();
		String collectionName;
		try {
			collectionName = EntityUtils.documentAnnotationValue(ApiCallStatsEntity.class);
		} catch (Exception e) {
			throw new BizException("Get ApiCallStatsEntity's collection name failed", e);
		}
		MongoCollection<org.bson.Document> collection = repository.getMongoOperations().getCollection(collectionName);
		List<Document> pipeline = new ArrayList<>();
		if (StringUtils.isNotBlank(userId)) {
			pipeline.add(new Document("$match", new Document("user_id", userId)));
		}
		pipeline.addAll(Arrays.asList(new Document("$facet",
				new Document("callTotalCount", Arrays.asList(new Document("$group", new Document("_id", null).append("data", new Document("$sum", "$callTotalCount")))))
						.append("transferDataTotalBytes", Arrays.asList(new Document("$group", new Document("_id", null).append("data", new Document("$sum", "$transferDataTotalBytes")))))
						.append("callAlarmTotalCount", Arrays.asList(new Document("$group", new Document("_id", null).append("data", new Document("$sum", "$callAlarmTotalCount")))))
						.append("responseDataRowTotalCount", Arrays.asList(new Document("$group", new Document("_id", "$allPathId").append("data", new Document("$sum", "$responseDataRowTotalCount")))))
						.append("totalResponseTime", Arrays.asList(new Document("$group", new Document("_id", "$allPathId").append("data", new Document("$sum", "$totalResponseTime")))))
						.append("alarmApiTotalCount", Arrays.asList(new Document("$match", new Document("accessFailureRate", new Document("$gt", 0))),
								new Document("$group", new Document("_id", null).append("data", new Document("$sum", 1L)))))
						.append("lastUpdAt", Arrays.asList(new Document("$group", new Document("_id", null).append("data", new Document("$max", "$last_updated")))))
		)));
		if (log.isDebugEnabled()) {
			StringBuilder pipelineString = new StringBuilder();
			pipeline.forEach(document -> pipelineString.append(document.toJson()).append(System.lineSeparator()));
			log.debug("ApiCallStatsService.aggregateByUserId pipeline: {}{}", System.lineSeparator(), pipelineString);
		}
		try (
				MongoCursor<Document> iterator = collection.aggregate(pipeline).allowDiskUse(true).iterator()
		) {
			if (iterator.hasNext()) {
				Document doc = iterator.next();
				if (log.isDebugEnabled()) {
					log.debug("ApiCallStatsService.aggregateByUserId doc: {}", doc.toJson());
				}
				List<?> tempList;
				// callTotalCount
				Object callTotalCount = doc.get("callTotalCount");
				if (callTotalCount instanceof List) {
					tempList = (List<?>) callTotalCount;
					if (!tempList.isEmpty()) {
						apiCallStatsDto.setCallTotalCount(DocumentUtils.getLong((Document) tempList.get(0), "data"));
					}
				}
				// transferDataTotalBytes
				Object transferDataTotalBytes = doc.get("transferDataTotalBytes");
				if (transferDataTotalBytes instanceof List) {
					tempList = (List<?>) transferDataTotalBytes;
					if (!tempList.isEmpty()) {
						apiCallStatsDto.setTransferDataTotalBytes(DocumentUtils.getLong((Document) tempList.get(0), "data"));
					}
				}
				// callAlarmTotalCount
				Object callAlarmTotalCount = doc.get("callAlarmTotalCount");
				if (callAlarmTotalCount instanceof List) {
					tempList = (List<?>) callAlarmTotalCount;
					if (!tempList.isEmpty()) {
						apiCallStatsDto.setCallAlarmTotalCount(DocumentUtils.getLong((Document) tempList.get(0), "data"));
					}
				}
				// responseDataRowTotalCount
				Object responseDataRowTotalCount = doc.get("responseDataRowTotalCount");
				if (responseDataRowTotalCount instanceof List) {
					tempList = (List<?>) responseDataRowTotalCount;
					if (!tempList.isEmpty()) {
						apiCallStatsDto.setResponseDataRowTotalCount(DocumentUtils.getLong((Document) tempList.get(0), "data"));
					}
				}
				// totalResponseTime
				Object totalResponseTime = doc.get("totalResponseTime");
				if (totalResponseTime instanceof List) {
					tempList = (List<?>) totalResponseTime;
					if (!tempList.isEmpty()) {
						apiCallStatsDto.setTotalResponseTime(DocumentUtils.getLong((Document) tempList.get(0), "data"));
					}
				}
				// alarmApiTotalCount
				Object alarmApiTotalCount = doc.get("alarmApiTotalCount");
				if (alarmApiTotalCount instanceof List) {
					tempList = (List<?>) alarmApiTotalCount;
					if (!tempList.isEmpty()) {
						apiCallStatsDto.setAlarmApiTotalCount(DocumentUtils.getLong((Document) tempList.get(0), "data"));
					}
				}
				// max last update time
				Object lastUpdAt = doc.get("lastUpdAt");
				if (lastUpdAt instanceof List) {
					tempList = (List<?>) lastUpdAt;
					if (!tempList.isEmpty()) {
						Object data = ((Document) tempList.get(0)).get("data");
						if (data instanceof Date) {
							apiCallStatsDto.setLastUpdAt((Date) data);
						}
					}
				}
			}
		}
		return apiCallStatsDto;
	}

	public boolean isEmpty() {
		Query query = new Query().limit(1);
		query.fields().include("_id");
		return !repository.findOne(query).isPresent();
	}

	public void deleteAllByModuleId(String moduleId) {
		Query deleteQuery = Query.query(Criteria.where("moduleId").is(moduleId));
		deleteAll(deleteQuery);
	}
}