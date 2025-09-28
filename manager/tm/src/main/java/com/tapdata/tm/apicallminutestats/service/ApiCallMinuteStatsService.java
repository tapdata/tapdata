package com.tapdata.tm.apicallminutestats.service;

import com.tapdata.tm.apicallminutestats.dto.ApiCallMinuteStatsDto;
import com.tapdata.tm.apicallminutestats.entity.ApiCallMinuteStatsEntity;
import com.tapdata.tm.apicallminutestats.repository.ApiCallMinuteStatsRepository;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * @Author:
 * @Date: 2024/08/29
 * @Description:
 */
@Service
@Slf4j
public class ApiCallMinuteStatsService extends BaseService<ApiCallMinuteStatsDto, ApiCallMinuteStatsEntity, ObjectId, ApiCallMinuteStatsRepository> {
	public ApiCallMinuteStatsService(@NonNull ApiCallMinuteStatsRepository repository) {
		super(repository, ApiCallMinuteStatsDto.class, ApiCallMinuteStatsEntity.class);
	}

	protected void beforeSave(ApiCallMinuteStatsDto apiCallMinuteStats, UserDetail user) {

	}

	public void merge(List<ApiCallMinuteStatsDto> apiCallMinuteStatsDtoList) {
		// Just check and merge first record
		if (apiCallMinuteStatsDtoList == null || apiCallMinuteStatsDtoList.isEmpty()) {
			return;
		}
		ApiCallMinuteStatsDto apiCallMinuteStatsDto = apiCallMinuteStatsDtoList.get(0);
		Query query = Query.query(Criteria.where("moduleId").is(apiCallMinuteStatsDto.getModuleId())
				.and("apiCallTime").is(apiCallMinuteStatsDto.getApiCallTime()));
		ApiCallMinuteStatsDto existsApiCallMinuteStats = findOne(query);
		if (null != existsApiCallMinuteStats) {
			apiCallMinuteStatsDto.setTotalResponseTime(apiCallMinuteStatsDto.getTotalResponseTime() + existsApiCallMinuteStats.getTotalResponseTime());
			apiCallMinuteStatsDto.setResponseDataRowTotalCount(apiCallMinuteStatsDto.getResponseDataRowTotalCount() + existsApiCallMinuteStats.getResponseDataRowTotalCount());
			apiCallMinuteStatsDto.setTransferDataTotalBytes(apiCallMinuteStatsDto.getTransferDataTotalBytes() + existsApiCallMinuteStats.getTransferDataTotalBytes());
			// Recalculate responseTimePerRow and rowPerSecond
			calculate(apiCallMinuteStatsDto);
			apiCallMinuteStatsDto.setId(existsApiCallMinuteStats.getId());
			apiCallMinuteStatsDto.setCreateAt(existsApiCallMinuteStats.getCreateAt());
		}
	}

	public void calculate(ApiCallMinuteStatsDto apiCallMinuteStatsDto) {
		if (null == apiCallMinuteStatsDto) {
			return;
		}
		if (apiCallMinuteStatsDto.getResponseDataRowTotalCount() <= 0 || apiCallMinuteStatsDto.getTotalResponseTime() <= 0) {
			return;
		}
		BigDecimal responseTimePerRow = BigDecimal.valueOf(apiCallMinuteStatsDto.getTotalResponseTime()).divide(BigDecimal.valueOf(apiCallMinuteStatsDto.getResponseDataRowTotalCount()), 2, RoundingMode.HALF_UP);
		BigDecimal rowPerSecond = BigDecimal.valueOf(apiCallMinuteStatsDto.getTransferDataTotalBytes()).divide(BigDecimal.valueOf(apiCallMinuteStatsDto.getTotalResponseTime()), 2, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(1000));
		apiCallMinuteStatsDto.setResponseTimePerRow(responseTimePerRow.doubleValue());
		apiCallMinuteStatsDto.setTransferBytePerSecond(rowPerSecond.doubleValue());
	}

	/**
	 * @deprecated
	 * @description use bulkWrite(List<ApiCallMinuteStatsDto> apiCallMinuteStatsDtoList, Class<ApiCallMinuteStatsEntity> entityClass, Function<ApiCallMinuteStatsDto, Query> queryFunction)
	 * */
	@Deprecated(since = "release-v4.9.0", forRemoval = true)
	public void bulkWrite(List<ApiCallMinuteStatsDto> apiCallMinuteStatsDtoList) {
		if (null == apiCallMinuteStatsDtoList || apiCallMinuteStatsDtoList.isEmpty()) {
			return;
		}
		BulkOperations bulkOperations = repository.bulkOperations(BulkOperations.BulkMode.UNORDERED);
		List<ApiCallMinuteStatsEntity> apiCallMinuteStatsEntities = convertToEntity(ApiCallMinuteStatsEntity.class, apiCallMinuteStatsDtoList);
		for (ApiCallMinuteStatsEntity apiCallMinuteStatsEntity : apiCallMinuteStatsEntities) {
			Update update = repository.buildUpdateSet(apiCallMinuteStatsEntity);
			Query query = Query.query(Criteria.where("id").is(apiCallMinuteStatsEntity.getId()));
			bulkOperations.upsert(query, update);
		}
		bulkOperations.execute();
	}

	public boolean isEmpty() {
		Query query = new Query().limit(1);
		query.fields().include("_id");
		return !repository.findOne(query).isPresent();
	}
}