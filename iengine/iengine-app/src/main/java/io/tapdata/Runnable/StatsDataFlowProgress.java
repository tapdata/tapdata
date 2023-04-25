package io.tapdata.Runnable;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.InitialStat;
import com.tapdata.entity.Job;
import com.tapdata.entity.Stats;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.entity.dataflow.DataFlowInsight;
import com.tapdata.entity.dataflow.DataFlowProgressDetail;
import com.tapdata.entity.dataflow.Overview;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-12-01 17:36
 **/
public class StatsDataFlowProgress implements Runnable {

	private Logger logger = LogManager.getLogger(StatsDataFlowProgress.class);
	private DataFlow dataFlow;
	private ClientMongoOperator clientMongoOperator;

	public StatsDataFlowProgress(DataFlow dataFlow, ClientMongoOperator clientMongoOperator) {
		this.dataFlow = dataFlow;
		this.clientMongoOperator = clientMongoOperator;
	}

	@Override
	public void run() {
		try {
			String dataFlowId = dataFlow.getId();
			Overview overview = new Overview();
			HashSet sourceTable = new HashSet();
			HashSet targetTable = new HashSet();
			// key: source connectionId+"-"+target connectionId
			Map<String, DataFlowProgressDetail> processDetailGroupByDB = new HashMap<>();
			Map<String, HashSet> sourceTableGroupByDb = new HashMap<>();
			Map<String, HashSet> targetTableGroupByDb = new HashMap<>();

			try {
				Query query = new Query(Criteria.where("dataFlowId").is(dataFlowId));
				query.fields().include("stats").include("status");
				List<Job> jobs = clientMongoOperator.find(query, ConnectorConstant.JOB_COLLECTION, Job.class);
				if (CollectionUtils.isEmpty(jobs)) {
					return;
				}

				Integer doneCount = 0;
				for (Job job : jobs) {
					if (Thread.interrupted()) {
						break;
					}
					if (job.getStats() == null) {
						continue;
					}

					Stats stats = job.getStats();
					if (!stats.isInitialStatsEnable()) {
						continue;
					}
					if (CollectionUtils.isNotEmpty(stats.getInitialStats())) {
						List<InitialStat> initialStats = stats.getInitialStats();
						for (InitialStat initialStat : initialStats) {
							if (Thread.interrupted()) {
								break;
							}
							if (dataFlow.getStatus().equals(ConnectorConstant.FORCE_STOPPING)) {
								break;
							}
							if (null == initialStat.getSourceConnectionId() || null == initialStat.getTargetConnectionId()) {
								continue;
							}
							DataFlowInsight dataFlowInsight = new DataFlowInsight();
							dataFlowInsight.setDataFlowId(dataFlowId);
							dataFlowInsight.setStatsType(DataFlowInsight.StatsTypeEnum.DATAFLOW_DETAILS_STATS.getType());

							DataFlowProgressDetail dataFlowProgressDetail = new DataFlowProgressDetail();
							dataFlowProgressDetail.setSourceConnectionId(initialStat.getSourceConnectionId());
							dataFlowProgressDetail.setSourceConnectionName(initialStat.getSourceConnectionName());
							dataFlowProgressDetail.setSourceTableName(initialStat.getSourceTableName());
							dataFlowProgressDetail.setSourceRowNum(initialStat.getSourceRowNum());
							dataFlowProgressDetail.setTargetConnectionId(initialStat.getTargetConnectionId());
							dataFlowProgressDetail.setTargetConnectionName(initialStat.getTargetConnectionName());
							dataFlowProgressDetail.setTargetTableName(initialStat.getTargetTableName());
							dataFlowProgressDetail.setTargetRowNum(initialStat.getTargetRowNum());
							dataFlowProgressDetail.setSourceDbName(initialStat.getSourceDbName());
							dataFlowProgressDetail.setTargetDbName(initialStat.getTargetDbName());
							if (dataFlowProgressDetail.getSourceRowNum().compareTo(0L) < 0) {
								dataFlowProgressDetail.setStatus(DataFlowProgressDetail.Status.waiting);
							} else {
								dataFlowProgressDetail.setStatus(initialStat.getStatus());
							}
							dataFlowProgressDetail.setStartTime(initialStat.getStartTime());
							dataFlowProgressDetail.setSourceDatabaseType(initialStat.getSourceDatabaseType());
							dataFlowProgressDetail.setTargetDatabaseType(initialStat.getTargetDatabaseType());

							dataFlowInsight.setStatsData(dataFlowProgressDetail);

							Query insightQuery = new Query(
									Criteria.where("dataFlowId").regex(dataFlowId)
											.and("statsType").is(DataFlowInsight.StatsTypeEnum.DATAFLOW_DETAILS_STATS.getType())
											.and("statsData.sourceConnectionId").regex(dataFlowProgressDetail.getSourceConnectionId())
											.and("statsData.targetConnectionId").regex(dataFlowProgressDetail.getTargetConnectionId())
											.and("statsData.sourceTableName").is(dataFlowProgressDetail.getSourceTableName())
											.and("statsData.targetTableName").is(dataFlowProgressDetail.getTargetTableName())
							);

							// upsert into DataFlowInsights
							List<DataFlowInsight> dataFlowInsights = clientMongoOperator.find(insightQuery, ConnectorConstant.DATA_FLOW_INSIGHT_COLLECTION, DataFlowInsight.class);
							if (CollectionUtils.isNotEmpty(dataFlowInsights)) {
								DataFlowInsight firstDataFlowInsight = dataFlowInsights.get(0);
								clientMongoOperator.update(new Query(Criteria.where("_id").is(firstDataFlowInsight.getId())),
										new Update()
												.set("statsData.sourceRowNum", dataFlowProgressDetail.getSourceRowNum())
												.set("statsData.targetRowNum", dataFlowProgressDetail.getTargetRowNum())
												.set("statsData.status", dataFlowProgressDetail.getStatus())
												.set("statsData.startTime", dataFlowProgressDetail.getStartTime()), ConnectorConstant.DATA_FLOW_INSIGHT_COLLECTION);
							} else {
								clientMongoOperator.insertOne(MapUtil.obj2Map(dataFlowInsight), ConnectorConstant.DATA_FLOW_INSIGHT_COLLECTION);
							}

							// Group by source+sink connection
							String dbKey = initialStat.getSourceConnectionId() + "-" + initialStat.getTargetConnectionId();
							DataFlowProgressDetail dataFlowProgressDetailGroupByDB;
							if (processDetailGroupByDB.containsKey(dbKey)) {
								dataFlowProgressDetailGroupByDB = processDetailGroupByDB.get(dbKey);
							} else {
								dataFlowProgressDetailGroupByDB = new DataFlowProgressDetail();
								dataFlowProgressDetailGroupByDB.setSourceConnectionId(dataFlowProgressDetail.getSourceConnectionId());
								dataFlowProgressDetailGroupByDB.setTargetConnectionId(dataFlowProgressDetail.getTargetConnectionId());
								dataFlowProgressDetailGroupByDB.setSourceConnectionName(dataFlowProgressDetail.getSourceConnectionName());
								dataFlowProgressDetailGroupByDB.setTargetConnectionName(dataFlowProgressDetail.getTargetConnectionName());
								dataFlowProgressDetailGroupByDB.setSourceDbName(dataFlowProgressDetail.getSourceDbName());
								dataFlowProgressDetailGroupByDB.setTargetDbName(dataFlowProgressDetail.getTargetDbName());
								dataFlowProgressDetailGroupByDB.setSourceRowNum(-1L);
								dataFlowProgressDetailGroupByDB.setTargetRowNum(0L);
								dataFlowProgressDetailGroupByDB.setSourceTableNum(0);
								dataFlowProgressDetailGroupByDB.setTargetTableNum(0);
								dataFlowProgressDetailGroupByDB.setStatus(dataFlowProgressDetail.getStatus());
								dataFlowProgressDetailGroupByDB.setSourceDatabaseType(dataFlowProgressDetail.getSourceDatabaseType());
								dataFlowProgressDetailGroupByDB.setTargetDatabaseType(dataFlowProgressDetail.getTargetDatabaseType());
								processDetailGroupByDB.put(dbKey, dataFlowProgressDetailGroupByDB);
							}
							if (dataFlowProgressDetail.getSourceRowNum().compareTo(0L) > 0) {
								dataFlowProgressDetailGroupByDB.setSourceRowNum(dataFlowProgressDetailGroupByDB.getSourceRowNum() + dataFlowProgressDetail.getSourceRowNum());
							}
							if (dataFlowProgressDetail.getTargetRowNum().compareTo(0L) >= 0) {
								dataFlowProgressDetailGroupByDB.setTargetRowNum(dataFlowProgressDetailGroupByDB.getTargetRowNum() + dataFlowProgressDetail.getTargetRowNum());
							}
							if (!dataFlowProgressDetail.getStatus().equals(DataFlowProgressDetail.Status.waiting)) {
								dataFlowProgressDetailGroupByDB.setStatus(dataFlowProgressDetail.getStatus());
							}
							HashSet sourceTableGroupByDbSet;
							HashSet targetTableGroupByDbSet;
							if (sourceTableGroupByDb.containsKey(dbKey)) {
								sourceTableGroupByDbSet = sourceTableGroupByDb.get(dbKey);
							} else {
								sourceTableGroupByDbSet = new HashSet();
								sourceTableGroupByDb.put(dbKey, sourceTableGroupByDbSet);
							}
							if (targetTableGroupByDb.containsKey(dbKey)) {
								targetTableGroupByDbSet = targetTableGroupByDb.get(dbKey);
							} else {
								targetTableGroupByDbSet = new HashSet();
								targetTableGroupByDb.put(dbKey, targetTableGroupByDbSet);
							}
							sourceTableGroupByDbSet.add(dataFlowProgressDetail.getSourceTableName());
							targetTableGroupByDbSet.add(dataFlowProgressDetail.getTargetTableName());

							// Overview
							if (!sourceTable.contains(dataFlowProgressDetail.getSourceTableName()) && dataFlowProgressDetail.getSourceRowNum() >= 0) {
								if (overview.getSourceRowNum() < 0) {
									overview.setSourceRowNum(0L);
								}
								overview.setSourceRowNum(overview.getSourceRowNum() + dataFlowProgressDetail.getSourceRowNum());
							}
							if (!targetTable.contains(dataFlowProgressDetail.getTargetTableName())) {
								overview.setTargatRowNum(overview.getTargatRowNum() + dataFlowProgressDetail.getTargetRowNum());
							}

							sourceTable.add(dataFlowProgressDetail.getSourceConnectionId() + "_" + dataFlowProgressDetail.getSourceTableName());
							targetTable.add(dataFlowProgressDetail.getTargetConnectionId() + "_" + dataFlowProgressDetail.getTargetTableName());
							if (initialStat.getStatus().equals(DataFlowProgressDetail.Status.done)
									|| (initialStat.getSourceRowNum() == initialStat.getTargetRowNum() && initialStat.getSourceRowNum() == 0 && initialStat.getTargetRowNum() == 0)) {
								doneCount++;
							}
						}

						for (Map.Entry<String, DataFlowProgressDetail> entry : processDetailGroupByDB.entrySet()) {
							String dbKey = entry.getKey();
							DataFlowProgressDetail detail = entry.getValue();
							if (Thread.interrupted()) {
								return;
							}
							detail.setSourceTableNum(sourceTableGroupByDb.get(dbKey).size());
							detail.setTargetTableNum(targetTableGroupByDb.get(dbKey).size());
						}

						overview.setSourceTableNum(sourceTable.size() > 0 ? sourceTable.size() : overview.getSourceTableNum());
						overview.setTargetTableNum(targetTable.size());
						overview.setWaitingForSyecTableNums(Integer.parseInt(overview.getSourceTableNum() + "") - doneCount);
						if (doneCount.equals(overview.getSourceTableNum())) {
							overview.setStatus(DataFlowProgressDetail.Status.done);
						} else {
							overview.setStatus(DataFlowProgressDetail.Status.running);
						}
					} else {
						overview.setStatus(DataFlowProgressDetail.Status.done);
					}
				}
				dataFlow.getStats().setOverview(overview);
				dataFlow.getStats().setProgressGroupByDB(new ArrayList<>(processDetailGroupByDB.values()));
				clientMongoOperator.update(new Query(Criteria.where("_id").is(dataFlowId)),
						new Update().set("stats.overview", overview).set("stats.progressGroupByDB", processDetailGroupByDB.values()),
						ConnectorConstant.DATA_FLOW_COLLECTION);
			} catch (Exception e) {
				String err = "Stats data flow progress detail failed, err: " + e.getMessage() + "\n  " + Log4jUtil.getStackString(e);
				logger.error(err, e);
			}
		} finally {
		}
	}
}
