package com.tapdata.entity.dataflow;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.tapdata.constant.JSONUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.RandomUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jackin
 */
public class DataFlowStats implements Serializable {

	private static final long serialVersionUID = -4935489588106735126L;
	/**
	 * stage input stats
	 */
	public RuntimeThroughput input = new RuntimeThroughput();

	/**
	 * stage input stats
	 */
	public RuntimeThroughput output = new RuntimeThroughput();

	public RuntimeThroughput insert = new RuntimeThroughput();

	public RuntimeThroughput update = new RuntimeThroughput();

	public RuntimeThroughput delete = new RuntimeThroughput();

	/**
	 * total event transmission time
	 */
	public long transmissionTime;

	public long transTimeAvg;

	/**
	 * replicate lag
	 */
	public long replicationLag;

	private List<StageRuntimeStats> stagesMetrics;

	/**
	 * key: stageId
	 * value: dataCount
	 **/
	private List<Map<String, Object>> totalCount;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Overview overview;
	private List<DataFlowProgressDetail> progressGroupByDB;

	public DataFlowStats() {
	}

	public DataFlowStats(RuntimeThroughput input, RuntimeThroughput output, long transmissionTime, long replicationLag, List<StageRuntimeStats> stagesMetrics) {
		this.input = input;
		this.output = output;
		this.transmissionTime = transmissionTime;
		this.replicationLag = replicationLag;
		this.stagesMetrics = stagesMetrics;
	}

	public void initStageMetrics(List<Stage> stages) {
		stagesMetrics = new ArrayList<>();
		for (Stage stage : stages) {
			StageRuntimeStats stageRuntimeStats = new StageRuntimeStats();
			stageRuntimeStats.setStageId(stage.getId());
			stagesMetrics.add(stageRuntimeStats);
		}
	}

	public void calculate(DataFlowStats previousStats) {

		long deltaRows = this.input.getRows() - previousStats.getInput().getRows();
		long deltaTransTime = this.transmissionTime - previousStats.getTransmissionTime();
		this.transTimeAvg = deltaRows > 0 ? (long) Math.ceil(deltaTransTime / deltaRows) : 0;

	}

	public void collectStats(List<StageRuntimeStats> newStagesMetrics, List<Stage> stages) {


		if (CollectionUtils.isEmpty(stagesMetrics)) {
			this.stagesMetrics = newStagesMetrics;
		}

		Map<String, StageRuntimeStats> stageRuntimeStatsMap = new HashMap<>();
		for (StageRuntimeStats stageRuntimeStats : stagesMetrics) {
			stageRuntimeStatsMap.put(stageRuntimeStats.getStageId(), stageRuntimeStats);
		}

		for (StageRuntimeStats newStageMetrics : newStagesMetrics) {
			String stageId = newStageMetrics.getStageId();
			if (!stageRuntimeStatsMap.containsKey(stageId)) {
				stagesMetrics.add(newStageMetrics);
				stageRuntimeStatsMap.put(stageId, newStageMetrics);
			}

			stageRuntimeStatsMap.get(stageId).mergeStats(newStageMetrics);
			stageRuntimeStatsMap.get(stageId).setStatus(newStageMetrics.getStatus());

		}

		statsFlowMetric(stages, stageRuntimeStatsMap);

	}

	private void statsFlowMetric(List<Stage> stages, Map<String, StageRuntimeStats> stageRuntimeStatsMap) {
		List<Stage> srcStages = new ArrayList<>();
		List<Stage> tarStages = new ArrayList<>();
		Long flowReplLag = null;
		long flowTransTime = 0L;
		for (Stage stage : stages) {
			List<String> inputLanes = stage.getInputLanes();
			List<String> outputLanes = stage.getOutputLanes();
			if (CollectionUtils.isNotEmpty(inputLanes) && CollectionUtils.isNotEmpty(outputLanes)) {
				continue;
			}

			if (CollectionUtils.isNotEmpty(outputLanes)) {
				srcStages.add(stage);
			}

			if (CollectionUtils.isNotEmpty(inputLanes)) {
				tarStages.add(stage);
			}
		}


		for (StageRuntimeStats stageRuntimeStats : stageRuntimeStatsMap.values()) {
			if (flowReplLag == null) {
				flowReplLag = stageRuntimeStats.getReplicationLag();
			} else {
				flowReplLag = stageRuntimeStats.getReplicationLag() > flowReplLag ? flowReplLag : stageRuntimeStats.getReplicationLag();
			}

			flowTransTime += stageRuntimeStats.getTransmissionTime();
		}

		if (CollectionUtils.isNotEmpty(stagesMetrics)) {
			for (StageRuntimeStats stageRuntimeStats : stagesMetrics) {
				if (stageRuntimeStatsMap.containsKey(stageRuntimeStats.getStageId())) {
					stageRuntimeStats.setStatus(stageRuntimeStatsMap.get(stageRuntimeStats.getStageId()).getStatus());
				}
			}
		}

		this.replicationLag = flowReplLag;
		this.transmissionTime = flowTransTime;

		if (CollectionUtils.isNotEmpty(srcStages)) {
			this.input = new RuntimeThroughput();
			for (Stage srcStage : srcStages) {
				String stageId = srcStage.getId();
				StageRuntimeStats stageRuntimeStats = stageRuntimeStatsMap.get(stageId);
				if (stageRuntimeStats == null) {
					continue;
				}
				RuntimeThroughput output = stageRuntimeStats.getOutput();
				this.input.incrementStats(output);
			}
		}

		if (CollectionUtils.isNotEmpty(tarStages)) {
			this.output = new RuntimeThroughput();
			this.insert.makeZero();
			this.update.makeZero();
			this.delete.makeZero();
			for (Stage tarStage : tarStages) {
				String stageId = tarStage.getId();
				StageRuntimeStats stageRuntimeStats = stageRuntimeStatsMap.get(stageId);
				if (stageRuntimeStats == null) {
					continue;
				}
				RuntimeThroughput input = stageRuntimeStats.getInput();
				this.output.incrementStats(input);
				this.insert.incrementStats(stageRuntimeStats.getInsert());
				this.update.incrementStats(stageRuntimeStats.getUpdate());
				this.delete.incrementStats(stageRuntimeStats.getDelete());
			}
		}

	}

	public RuntimeThroughput getInput() {
		return input;
	}

	public long getTransTimeAvg() {
		return transTimeAvg;
	}

	public void setTransTimeAvg(long transTimeAvg) {
		this.transTimeAvg = transTimeAvg;
	}

	public void setInput(RuntimeThroughput input) {
		this.input = input;
	}

	public RuntimeThroughput getOutput() {
		return output;
	}

	public void setOutput(RuntimeThroughput output) {
		this.output = output;
	}

	public long getTransmissionTime() {
		return transmissionTime;
	}

	public void setTransmissionTime(long transmissionTime) {
		this.transmissionTime = transmissionTime;
	}

	public long getReplicationLag() {
		return replicationLag;
	}

	public void setReplicationLag(long replicationLag) {
		this.replicationLag = replicationLag;
	}

	public List<StageRuntimeStats> getStagesMetrics() {
		return stagesMetrics;
	}

	public void setStagesMetrics(List<StageRuntimeStats> stagesMetrics) {
		this.stagesMetrics = stagesMetrics;
	}

	public List<Map<String, Object>> getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(List<Map<String, Object>> totalCount) {
		this.totalCount = totalCount;
	}

	public Overview getOverview() {
		return overview;
	}

	public void setOverview(Overview overview) {
		this.overview = overview;
	}

	public List<DataFlowProgressDetail> getProgressGroupByDB() {
		return progressGroupByDB;
	}

	public void setProgressGroupByDB(List<DataFlowProgressDetail> progressGroupByDB) {
		this.progressGroupByDB = progressGroupByDB;
	}

	public static void main(String[] args) throws JsonProcessingException {
		List<String> stageIds = Arrays.asList("cfe03589-3548-4f23-9684-7dcbab2ef24e", "0990f02e-4f5b-43aa-8549-99e28477323e");

		RuntimeThroughput input = new RuntimeThroughput(RandomUtils.nextLong(0, 100000L), RandomUtils.nextLong(0, 100000L));
		RuntimeThroughput output = new RuntimeThroughput(RandomUtils.nextLong(0, 100000L), RandomUtils.nextLong(0, 100000L));
		long transmissionTime = RandomUtils.nextLong(0, 100000L);
		long replicationLag = RandomUtils.nextLong(0, 100000L);

		List<StageRuntimeStats> stagesMetrics = new ArrayList<>();
		for (String stageId : stageIds) {
			RuntimeThroughput stageInput = new RuntimeThroughput(RandomUtils.nextLong(0, 100000L), RandomUtils.nextLong(0, 100000L));
			RuntimeThroughput stageOutput = new RuntimeThroughput(RandomUtils.nextLong(0, 100000L), RandomUtils.nextLong(0, 100000L));
			long stgTransTime = RandomUtils.nextLong(0, 100000L);
			long stgReplLag = RandomUtils.nextLong(0, 100000L);

			stagesMetrics.add(new StageRuntimeStats(stageId, stageInput, stageOutput, stgTransTime, stgReplLag));

		}

		DataFlowStats dataFlowStats = new DataFlowStats(input, output, transmissionTime, replicationLag, stagesMetrics);
		String s = JSONUtil.obj2Json(dataFlowStats);
		System.out.println(s);
	}
}
