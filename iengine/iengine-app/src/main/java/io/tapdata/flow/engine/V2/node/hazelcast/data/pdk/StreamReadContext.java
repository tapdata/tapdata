package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import io.tapdata.async.master.ParallelWorker;

import java.util.List;

/**
 * @author aplomb
 */
public class StreamReadContext {
	private ParallelWorker partitionsReader;
	//	public StreamReadContext partitionsReader(AsyncParallelWorker partitionsReader) {
//		this.partitionsReader = partitionsReader;
//		return this;
//	}
	private List<String> tables;

	public StreamReadContext tables(List<String> tables) {
		this.tables = tables;
		return this;
	}

	private boolean streamStage;

	public StreamReadContext streamStage(boolean streamStage) {
		this.streamStage = streamStage;
		return this;
	}

	public static StreamReadContext create() {
		return new StreamReadContext();
	}

//	public AsyncParallelWorker getPartitionsReader() {
//		return partitionsReader;
//	}
//
//	public void setPartitionsReader(AsyncParallelWorker partitionsReader) {
//		this.partitionsReader = partitionsReader;
//	}

	public List<String> getTables() {
		return tables;
	}

	public void setTables(List<String> tables) {
		this.tables = tables;
	}

	public boolean isStreamStage() {
		return streamStage;
	}

	public void setStreamStage(boolean streamStage) {
		this.streamStage = streamStage;
	}
}
