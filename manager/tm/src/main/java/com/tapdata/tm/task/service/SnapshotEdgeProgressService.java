package com.tapdata.tm.task.service;

import cn.hutool.core.lang.Assert;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.progress.BatchOperationDto;
import com.tapdata.tm.commons.task.dto.progress.TaskSnapshotProgress;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.task.bean.FullSyncVO;
import com.tapdata.tm.task.bean.TableStatus;
import com.tapdata.tm.task.entity.SnapshotEdgeProgressEntity;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.repository.SnapshotEdgeProgressRepository;
import com.tapdata.tm.utils.MongoUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2021/11/03
 * @Description:
 */
@Service
@Slf4j
public class SnapshotEdgeProgressService extends BaseService<TaskSnapshotProgress, SnapshotEdgeProgressEntity, ObjectId, SnapshotEdgeProgressRepository> {

	@Autowired
	private DataSourceService dataSourceService;

	public SnapshotEdgeProgressService(@NonNull SnapshotEdgeProgressRepository repository) {
		super(repository, TaskSnapshotProgress.class, SnapshotEdgeProgressEntity.class);
	}
   @Override
	protected void beforeSave(TaskSnapshotProgress TaskRuntimeInfoDto, UserDetail user) {
	}


	public List<TaskSnapshotProgress> save(List<TaskSnapshotProgress> dtoList) {
		Assert.notNull(dtoList, "Dto must not be null!");

		List<SnapshotEdgeProgressEntity> entityList = new ArrayList<>();
		for (TaskSnapshotProgress dto : dtoList) {
			SnapshotEdgeProgressEntity entity = convertToEntity(SnapshotEdgeProgressEntity.class, dto);
			entityList.add(entity);
		}

		entityList = repository.saveAll(entityList);

		dtoList = convertToDto(entityList, dtoClass);

		return dtoList;
	}

	@Override
	public TaskSnapshotProgress save(TaskSnapshotProgress dto, UserDetail userDetail) {

		Assert.notNull(dto, "Dto must not be null!");

		beforeSave(dto, userDetail);

		SnapshotEdgeProgressEntity entity = convertToEntity(entityClass, dto);

		entity = repository.save(entity);

		BeanUtils.copyProperties(entity, dto);

		return dto;
	}

	public long count(Where where) {
		Criteria criteria = repository.whereToCriteria(where);
		return repository.count(new Query(criteria));
	}

	public TaskSnapshotProgress findOne(Filter filter) {
		Query query = repository.filterToQuery(filter);
		return repository.findOne(query).map(entity -> convertToDto(entity, dtoClass)).orElse(null);
	}

	@Override
	public long updateByWhere(Where where, TaskSnapshotProgress dto, UserDetail userDetail) {

		beforeSave(dto, userDetail);
		Filter filter = new Filter(where);
		filter.setLimit(0);
		filter.setSkip(0);
		Query query = repository.filterToQuery(filter);
		SnapshotEdgeProgressEntity entity = convertToEntity(entityClass, dto);
		Update update = repository.buildUpdateSet(entity);
		UpdateResult updateResult = repository.update(query, update);
		return updateResult.getModifiedCount();
	}

	@Override
	public TaskSnapshotProgress upsertByWhere(Where where, TaskSnapshotProgress dto, UserDetail userDetail) {

		beforeSave(dto, userDetail);
		Filter filter = new Filter(where);
		filter.setLimit(0);
		filter.setSkip(0);
		Query query = repository.filterToQuery(filter);
		repository.upsert(query, convertToEntity(entityClass, dto));
		Optional<SnapshotEdgeProgressEntity> optional = repository.findOne(query);

		return optional.map(entity -> convertToDto(entity, dtoClass)).orElse(null);
	}

	/**
	 * 子任务同步概览信息
	 *
	 * @param taskId
	 * @return
	 */
	public FullSyncVO syncOverview(String taskId) {
		MongoTemplate mongoOperations = repository.getMongoOperations();

		Query queryTask = new Query(Criteria.where("_id").is(MongoUtils.toObjectId(taskId)));
		TaskEntity taskDto = mongoOperations.findOne(queryTask, TaskEntity.class);
		if (taskDto == null) {
			return null;
		}

		//通过子任务id字段，查询snapshot表中的
		Criteria criteria = Criteria.where("taskId").is(taskId)
				.and("type").is(TaskSnapshotProgress.ProgressType.TASK_PROGRESS.name());
		Query query = new Query(criteria);
		TaskSnapshotProgress snapshotProgress = findOne(query);
		if (snapshotProgress == null) {
			return null;
		}

	    FullSyncVO fullSyncVO = new FullSyncVO();
		BeanUtils.copyProperties(snapshotProgress, fullSyncVO);
		fullSyncVO.setCurrentTime(new Date());
		fullSyncVO.setStartTs(new Date(snapshotProgress.getStartTs()));
		if (snapshotProgress.getEndTs() != null) {
			fullSyncVO.setEndTs(new Date(snapshotProgress.getEndTs()));
		}
		fullSyncVO.setTotalDataNum(snapshotProgress.getWaitForRunNumber());
		double process;
		if (snapshotProgress.getWaitForRunNumber() == -1) {
			process = 0.0;
		} else if (snapshotProgress.getWaitForRunNumber() == 0) {
			process = 1.0;
		} else {
			process = snapshotProgress.getFinishNumber() / (snapshotProgress.getWaitForRunNumber() * 1.0d);
		}
		int proInt = (int) (process * 100);
		fullSyncVO.setProgress((double) proInt);

		//当前时间不能用。如果运行中的话

		DAG dag = taskDto.getDag();
		List<Node> sources = dag.getSources();
		if (CollectionUtils.isNotEmpty(sources)) {
			Node node = sources.get(0);
			String nodeId = node.getId();
			Criteria criteria1 = Criteria.where("tags.taskId").is(taskId).and("tags.type").is("node")
					.and("tags.nodeId").is(nodeId);
			Query query1 = new Query(criteria1);
			query1.with(Sort.by(Sort.Order.desc("date")));
			int outputQps = 0;
//			MeasurementEntity entity = mongoOperations.findOne(query1, MeasurementEntity.class, TableNameEnum.AgentMeasurement.getValue());
//			if (entity != null) {
//				List<Sample> samples = entity.getSamples();
//				if (CollectionUtils.isNotEmpty(samples)) {
//					samples.sort(Comparator.comparing(Sample::getDate).reversed());
//					Sample sample = samples.get(0);
//					Map<String, Number> vs = sample.getVs();
//					if (vs != null) {
//						Number outputQps1 = vs.get("outputQPS");
//						outputQps = outputQps1.intValue();
//					}
//				}
//			}

			if (outputQps == 0 || !TaskDto.STATUS_RUNNING.equals(taskDto.getStatus())) {
				fullSyncVO.setFinishDuration(-1L);
			} else {
				long num = fullSyncVO.getTotalDataNum() - fullSyncVO.getFinishNumber();
				fullSyncVO.setFinishDuration(num / outputQps);
			}
		}
		return fullSyncVO;
	}

	/**
	 * 任务同步的表状态信息
	 *
	 * @param taskId
	 * @return
	 */
	public Page<TableStatus> syncTableView(String taskId, long skip, int limit) {
		Criteria criteria = Criteria.where("taskId").is(taskId)
				.and("type").is(TaskSnapshotProgress.ProgressType.EDGE_PROGRESS.name());
		Query query = new Query(criteria);

		query.skip(skip);
		query.limit(limit);
		query.with(Sort.by(Sort.Order.asc("status")));
		List<TaskSnapshotProgress> taskSnapshotProgresses = findAll(query);

		List<TableStatus> tableStatuses = new ArrayList<>();
		List<String> srcConIds = taskSnapshotProgresses.stream().map(TaskSnapshotProgress::getSrcConnId).collect(Collectors.toList());
		List<String> tgtConIds = taskSnapshotProgresses.stream().map(TaskSnapshotProgress::getTgtConnId).collect(Collectors.toList());
		srcConIds.addAll(tgtConIds);
		Criteria idCriteria = Criteria.where("_id").in(srcConIds);
		Query query1 = new Query(idCriteria);
		query1.fields().include("name");
		List<DataSourceConnectionDto> dataSourceList = dataSourceService.findAll(query1);
		Map<String, String> connectNameMap = dataSourceList.stream().collect(Collectors.toMap(d -> d.getId().toHexString(), DataSourceConnectionDto::getName));
		for (TaskSnapshotProgress snapshotProgress : taskSnapshotProgresses) {
			TableStatus tableStatus = new TableStatus();
			BeanUtils.copyProperties(snapshotProgress, tableStatus);
			tableStatus.setStartTs(new Date(snapshotProgress.getStartTs()));
			tableStatus.setTotalNum(snapshotProgress.getWaitForRunNumber());
			tableStatus.setStatus(snapshotProgress.getStatus().name());
			tableStatus.setSrcName(connectNameMap.get(tableStatus.getSrcConnId()));
			tableStatus.setTgtName(connectNameMap.get(tableStatus.getTgtConnId()));
			double process;
			if (snapshotProgress.getWaitForRunNumber() == -1) {
				process = 0.0;
			} else if (snapshotProgress.getWaitForRunNumber() == 0) {
				process = 1.0;
			} else {
				process = snapshotProgress.getFinishNumber() / (snapshotProgress.getWaitForRunNumber() * 1.0d);
			}
			int proInt = (int) (process * 100);
			//process = proInt / 100d;
			tableStatus.setProgress((double) proInt);
			tableStatuses.add(tableStatus);
		}

		Page<TableStatus> page = new Page<>();
		page.setItems(tableStatuses);

		query.limit(0);
		query.skip(0);
		long count = count(query);
		page.setTotal(count);
		return page;
	}

	public void batchUpsert(List<BatchOperationDto> batchUpsertDtoList) {
		MongoTemplate mongoTemplate = repository.getMongoOperations();
		BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, SnapshotEdgeProgressEntity.class);

		for (BatchOperationDto batchUpsertDto : batchUpsertDtoList) {
			if (BatchOperationDto.BatchOp.insert.equals(batchUpsertDto.getOp())) {
				TaskSnapshotProgress document = batchUpsertDto.getDocument();
				ObjectId id = document.getId();
				if (id !=null) {
					Query query = new Query(Criteria.where("_id").is(id));
					SnapshotEdgeProgressEntity snapshotEdgeProgressEntity = convertToEntity(SnapshotEdgeProgressEntity.class, document);
					Update update = repository.buildUpdateSet(snapshotEdgeProgressEntity);
					bulkOperations.upsert(query, update);
				} else {
					bulkOperations.insert(batchUpsertDto.getDocument());
				}

			} else {
				if (StringUtils.isNotBlank(batchUpsertDto.getWhere())) {
					String whereJson = batchUpsertDto.getWhere();
					Where where = BaseController.parseWhere(whereJson);
					Criteria criteria = repository.whereToCriteria(where);
					Query query = new Query(criteria);
					if (BatchOperationDto.BatchOp.update.equals(batchUpsertDto.getOp())) {
						SnapshotEdgeProgressEntity entity = convertToEntity(SnapshotEdgeProgressEntity.class, batchUpsertDto.getDocument());
						Update update = repository.buildUpdateSet(entity);
						bulkOperations.updateOne(query, update);
					} else if (BatchOperationDto.BatchOp.delete.equals(batchUpsertDto.getOp())) {
						bulkOperations.remove(query);
					} else if (BatchOperationDto.BatchOp.upsert.equals(batchUpsertDto.getOp())) {
						SnapshotEdgeProgressEntity entity = convertToEntity(SnapshotEdgeProgressEntity.class, batchUpsertDto.getDocument());
						Update update = repository.buildUpdateSet(entity);
						bulkOperations.upsert(query, update);
					}
				}
			}
		}
		bulkOperations.execute();
	}
}
