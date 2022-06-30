package com.tapdata.entity.dataflow;

import java.io.Serializable;

/**
 * @author jackin
 */
public class StageRuntimeStats implements Serializable, Cloneable {

	private static final long serialVersionUID = 4760458538571533729L;
	private String stageId;

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

	private String status;

	public StageRuntimeStats() {
	}

	public StageRuntimeStats(String stageId, RuntimeThroughput input, RuntimeThroughput output, long transmissionTime, long replicationLag) {
		this.stageId = stageId;
		this.input = input;
		this.output = output;
		this.transmissionTime = transmissionTime;
		this.replicationLag = replicationLag;
	}

	public void speedCalculate(StageRuntimeStats lastStats) {

		long deltaRows = this.input.getRows() - lastStats.getInput().getRows();
		long deltaTransTime = this.transmissionTime - lastStats.getTransmissionTime();
		this.transTimeAvg = deltaRows > 0 ? (long) Math.ceil(deltaTransTime / deltaRows) : 0;
	}

	public void mergeStats(StageRuntimeStats stageRuntimeStats) {
		RuntimeThroughput input = stageRuntimeStats.getInput();
		this.input.setRows(input.getRows() > this.input.getRows() ? input.getRows() : this.input.getRows());
		this.input.setDataSize(input.getDataSize() > this.input.getDataSize() ? input.getDataSize() : this.input.getDataSize());

		RuntimeThroughput output = stageRuntimeStats.getOutput();
		this.output.setRows(output.getRows() > this.output.getRows() ? output.getRows() : this.output.getRows());
		this.output.setDataSize(output.getDataSize() > this.output.getDataSize() ? output.getDataSize() : this.output.getDataSize());

		RuntimeThroughput insert = stageRuntimeStats.getInsert();
		this.insert.setRows(insert.getRows() > this.insert.getRows() ? insert.getRows() : this.insert.getRows());
		this.insert.setDataSize(insert.getDataSize() > this.insert.getDataSize() ? insert.getDataSize() : this.insert.getDataSize());

		RuntimeThroughput update = stageRuntimeStats.getUpdate();
		this.update.setRows(update.getRows() > this.update.getRows() ? update.getRows() : this.update.getRows());
		this.update.setDataSize(update.getDataSize() > this.update.getDataSize() ? update.getDataSize() : this.update.getDataSize());

		RuntimeThroughput delete = stageRuntimeStats.getDelete();
		this.delete.setRows(delete.getRows() > this.delete.getRows() ? delete.getRows() : this.delete.getRows());
		this.delete.setDataSize(delete.getDataSize() > this.delete.getDataSize() ? delete.getDataSize() : this.delete.getDataSize());

		this.transmissionTime = stageRuntimeStats.getTransmissionTime() > this.transmissionTime ? stageRuntimeStats.getTransmissionTime() : this.transmissionTime;

		this.replicationLag = stageRuntimeStats.getReplicationLag();
	}

	public void incrementInput(int inputRows, long inputDataSize) {
		input.incrementRows(inputRows);
		input.incrementDataSize(inputDataSize);
	}

	public void incrementOuput(int outputRows, long outputDataSize) {
		output.incrementRows(outputRows);
		output.incrementDataSize(outputDataSize);
	}

	public void incrementInsert(RuntimeThroughput runtimeThroughput) {
		insert.incrementRows(runtimeThroughput.getRows());
		insert.incrementDataSize(runtimeThroughput.getDataSize());
	}

	public void incrementUpdate(RuntimeThroughput runtimeThroughput) {
		update.incrementRows(runtimeThroughput.getRows());
		update.incrementDataSize(runtimeThroughput.getDataSize());
	}

	public void incrementDelete(RuntimeThroughput runtimeThroughput) {
		delete.incrementRows(runtimeThroughput.getRows());
		delete.incrementDataSize(runtimeThroughput.getDataSize());
	}

	public void incrementInsert(int insertRows, long insertDataSize) {
		insert.incrementRows(insertRows);
		insert.incrementDataSize(insertDataSize);
	}

	public void incrementUpdate(int updateRows, long updateDataSize) {
		update.incrementRows(updateRows);
		update.incrementDataSize(updateDataSize);
	}

	public void incrementDelete(int deleteRows, long deleteDataSize) {
		delete.incrementRows(deleteRows);
		delete.incrementDataSize(deleteDataSize);
	}

	public void incrementTransTime(long transmissionTime) {
		this.transmissionTime += transmissionTime;
	}

	public String getStageId() {
		return stageId;
	}

	public void setStageId(String stageId) {
		this.stageId = stageId;
	}

	public RuntimeThroughput getInput() {
		return input;
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

	public RuntimeThroughput getInsert() {
		return insert;
	}

	public RuntimeThroughput getUpdate() {
		return update;
	}

	public RuntimeThroughput getDelete() {
		return delete;
	}

	public void setInsert(RuntimeThroughput insert) {
		this.insert = insert;
	}

	public void setUpdate(RuntimeThroughput update) {
		this.update = update;
	}

	public void setDelete(RuntimeThroughput delete) {
		this.delete = delete;
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

	public long getTransTimeAvg() {
		return transTimeAvg;
	}

	public void setTransTimeAvg(long transTimeAvg) {
		this.transTimeAvg = transTimeAvg;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	@Override
	public StageRuntimeStats clone() {
		StageRuntimeStats stageRuntimeStats = new StageRuntimeStats();
		stageRuntimeStats.setStageId(stageId);
		stageRuntimeStats.setTransTimeAvg(transTimeAvg);
		stageRuntimeStats.setTransmissionTime(this.transmissionTime);
		stageRuntimeStats.setReplicationLag(this.replicationLag);
		stageRuntimeStats.setInput(new RuntimeThroughput(this.input));
		stageRuntimeStats.setOutput(new RuntimeThroughput(this.output));
		stageRuntimeStats.setInsert(new RuntimeThroughput(this.insert));
		stageRuntimeStats.setUpdate(new RuntimeThroughput(this.update));
		stageRuntimeStats.setDelete(new RuntimeThroughput(this.delete));
		stageRuntimeStats.setStatus(status);

		return stageRuntimeStats;
	}


}
