package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.aspect.GetReadPartitionsFuncAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.async.master.AsyncParallelWorker;
import io.tapdata.async.master.JobContext;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePartitionReadDataNode;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.PDKSourceContext;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.ReadPartitionContext;
import io.tapdata.pdk.apis.partition.ReadPartition;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author aplomb
 */
public class PartitionConsumer implements Consumer<ReadPartition> {
	private PDKSourceContext pdkSourceContext;
	private TapTable tapTable;
	private AsyncParallelWorker partitionsReader;
	private GetReadPartitionsFuncAspect getReadPartitionsFuncAspect;
	private BatchReadFuncAspect batchReadFuncAspect;
	private List<ReadPartition> readPartitionList;
	private Map<String, TapEventPartitionDispatcher> tableEventPartitionDispatcher;
	private HazelcastSourcePartitionReadDataNode sourcePdkDataNodeEx1;
	public PartitionConsumer(PDKSourceContext pdkSourceContext, TapTable tapTable, AsyncParallelWorker partitionsReader, GetReadPartitionsFuncAspect getReadPartitionsFuncAspect, BatchReadFuncAspect batchReadFuncAspect, List<ReadPartition> readPartitionList, Map<String, TapEventPartitionDispatcher> tableEventPartitionDispatcher, HazelcastSourcePartitionReadDataNode sourcePdkDataNodeEx1) {
		this.partitionsReader = partitionsReader;
		this.pdkSourceContext = pdkSourceContext;
		this.readPartitionList = readPartitionList;
		this.getReadPartitionsFuncAspect = getReadPartitionsFuncAspect;
		this.batchReadFuncAspect = batchReadFuncAspect;
		this.tapTable = tapTable;
		this.tableEventPartitionDispatcher = tableEventPartitionDispatcher;
		this.sourcePdkDataNodeEx1 = sourcePdkDataNodeEx1;
	}
	@Override
	public void accept(ReadPartition readPartition) {
		readPartitionList.add(readPartition);
		if (getReadPartitionsFuncAspect != null)
			AspectUtils.accept(getReadPartitionsFuncAspect.state(GetReadPartitionsFuncAspect.STATE_READ_COMPLETE).getReadCompleteConsumers(), readPartition);

		readPartition.partitionIndex(tapTable.partitionIndex());
		ReadPartitionHandler readPartitionHandler = new ReadPartitionHandler(pdkSourceContext, tapTable, readPartition, sourcePdkDataNodeEx1);
		partitionsReader.job(readPartition.getId(),
				JobContext.create().context(ReadPartitionContext.create().pdkSourceContext(pdkSourceContext).table(tapTable).readPartition(readPartition).batchReadFuncAspect(batchReadFuncAspect)),
				asyncQueueWorker -> asyncQueueWorker.
						job("startCachingStreamData", jobContext -> {
							JobContext context = readPartitionHandler.handleStartCachingStreamData(jobContext);
							TapEventPartitionDispatcher dispatcher = tableEventPartitionDispatcher.get(tapTable.getId());
							if(dispatcher != null) {
								dispatcher.register(readPartition, readPartitionHandler);
							}
							return context;
						}).
						job("readPartition", readPartitionHandler::handleReadPartition).
						job("sendingDataFromPartition", readPartitionHandler::handleSendingDataFromPartition).
						job("finishedPartition", readPartitionHandler::handleFinishedPartition));
	}
}
