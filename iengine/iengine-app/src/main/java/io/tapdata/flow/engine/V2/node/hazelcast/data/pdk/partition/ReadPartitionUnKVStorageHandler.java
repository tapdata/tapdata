package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.async.master.JobContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePartitionReadDataNode;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.PDKSourceContext;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.ReadPartitionContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import static io.tapdata.entity.simplify.TapSimplify.insertRecordEvent;
import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * @author GavinXiao
 * @description ReadPartitionUnKVStorageHandler create by Gavin
 * @create 2023/4/6 14:25
 **/
public class ReadPartitionUnKVStorageHandler extends PartitionFieldParentHandler implements ReadPartitionHandler {
	private final PDKSourceContext pdkSourceContext;
	private final ReadPartition readPartition;
	private final HazelcastSourcePartitionReadDataNode sourcePdkDataNode;
	private final AtomicBoolean finished = new AtomicBoolean(false);
	private final LongAdder sentEventCount = new LongAdder();

	public ReadPartitionUnKVStorageHandler(PDKSourceContext pdkSourceContext, TapTable tapTable, ReadPartition readPartition, HazelcastSourcePartitionReadDataNode sourcePdkDataNode) {
		super(tapTable);
		this.readPartition = readPartition;
		this.pdkSourceContext = pdkSourceContext;
		this.sourcePdkDataNode = sourcePdkDataNode;
	}

	//[INFO ] 2023-04-07 11:39:56.336  [] job-file-log-6426c6a1c0253f6338896f7f - [Partition-Mongo-Dummy - Copy][BigData] - Stored the readPartition ReadPartition TapPartitionFilter rightBoundary _id<'6424344e01172f3ebd000000'; , takes 25060, storage takes 6400, filter takes 16226, total 1088484
	public JobContext handleReadPartition(JobContext jobContext) {
		ReadPartitionContext readPartitionContext = jobContext.getContext(ReadPartitionContext.class);
		sourcePdkDataNode.getObsLogger().info("Start storing partition {} into local, batchSize {} ", readPartition, sourcePdkDataNode.batchSize);
		QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = sourcePdkDataNode.getConnectorNode().getConnectorFunctions().getQueryByAdvanceFilterFunction();
		if (queryByAdvanceFilterFunction != null) {
			final List<TapEvent>[] reference = new List[]{new ArrayList<>()};
			long time = System.currentTimeMillis();
			LongAdder storageTakes = new LongAdder();
			LongAdder filterTakes = new LongAdder();
			LongAdder counter = new LongAdder();
//            LongAdder jetTakes = new LongAdder();
			BatchReadFuncAspect batchReadFuncAspect = readPartitionContext.getBatchReadFuncAspect();
			TapPartitionFilter partitionFilter = readPartition.getPartitionFilter();
			TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create()
					.match(partitionFilter.getMatch())
					.op(partitionFilter.getLeftBoundary())
					.op(partitionFilter.getRightBoundary())
					.batchSize(sourcePdkDataNode.batchSize >> 1);
			PDKMethodInvoker pdkMethodInvoker = sourcePdkDataNode.createPdkMethodInvoker();
			try {
				final long[] filterStart = new long[]{System.currentTimeMillis()};
				PDKInvocationMonitor.invoke(
						sourcePdkDataNode.getConnectorNode(),
						PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
						pdkMethodInvoker.runnable(
								() -> queryByAdvanceFilterFunction.query(
										sourcePdkDataNode.getConnectorNode().getConnectorContext(),
										tapAdvanceFilter,
										readPartitionContext.getTable(),
										filterResults -> {
											long storageTime = System.currentTimeMillis();
											filterTakes.add(storageTime - filterStart[0]);
											filterStart[0] = System.currentTimeMillis();
											List<Map<String, Object>> results = filterResults.getResults();
											if (null != results) {
												storageTime = System.currentTimeMillis();
												//jetTakes.add(sourcePdkDataNode.handleStreamInsertEventsReceived(results, null));
												for (Map<String, Object> result : results) {
													reference[0].add(insertRecordEvent(result, table).referenceTime(System.currentTimeMillis()));
													int size = reference[0].size();
													if (size >= tapAdvanceFilter.getBatchSize()) {
														enqueueTapEvents(batchReadFuncAspect, reference[0], sourcePdkDataNode);
//                                                    sourcePdkDataNode.handleStreamEventsReceived(reference[0], null);
//                                                    jetTakes.add();
														sentEventCount.add(size);
														counter.add(size);
														reference[0] = new ArrayList<>();
													}
												}
												storageTakes.add(System.currentTimeMillis() - storageTime);
											}
										}
								)
						)
				);
				if (!reference[0].isEmpty()) {
					int size = reference[0].size();
					enqueueTapEvents(batchReadFuncAspect, reference[0], sourcePdkDataNode);
//                    sourcePdkDataNode.handleStreamEventsReceived(reference[0], null);
					sentEventCount.add(size);
					counter.add(size);
					//reference[0] = new ArrayList<>();
				}
			} finally {
				sourcePdkDataNode.removePdkMethodInvoker(pdkMethodInvoker);
				sourcePdkDataNode.getObsLogger().info(
						"Stored the readPartition {}, " +
								"takes {}, " +
								"storage takes {}, " +
								"filter takes {}, " +
								"total {}",
						readPartition,
						(System.currentTimeMillis() - time),
						storageTakes.longValue(),
//                        jetTakes.longValue(),
						filterTakes.longValue(),
						counter.longValue());
			}
		}
		return null;
	}
	//======分片 单线程
	// takes 25060, storage takes 6400, filter takes 16226, total 1088484
	// takes 17011, storage takes 6363, filter takes 17009, total 1088484
	// takes 18072, storage takes 5272 of jet takes 2384, filter takes 13123, total 1104299

	//======分片 2线程
	// takes 30241, storage takes 12391, filter takes 25481, total 1072802
	// takes 34385, storage takes 12835, filter takes 30434, total 1088484
	// takes 58551, storage takes 29723, filter takes 53855, total 1707731
	// takes 62790, storage takes 30217, filter takes 62771, total 1874377
	// takes 23398, storage takes 13026, filter takes 23398, total 925115

	//======分片 4线程
	//takes 77683, storage takes 24258 of jet takes 18835, filter takes 35783, total 1004301
	//takes 78897, storage takes 66784 of jet takes 61110, filter takes 78858, total 1032305

	//takes 43957, storage takes 21668 of jet takes 16446, filter takes 34772, total 1004301
	//takes 45319, storage takes 31978 of jet takes 26020, filter takes 45276, total 1032305
	//takes 47175, storage takes 32979 of jet takes 17954, filter takes 47141, total 1104299
	//takes 98932, storage takes 74595 of jet takes 62090, filter takes 98848, total 1698947

	//takes 38790, storage takes 20479 of jet takes 15887, filter takes 30560, total 1004301
	//takes 39780, storage takes 29040 of jet takes 24210, filter takes 39763, total 1032305
	//takes 40972, storage takes 21704 of jet takes 16703, filter takes 40969, total 1078070
	//takes 41097, storage takes 29908 of jet takes 24876, filter takes 41064, total 1104299

	//takes 25075, storage takes 6047 of jet takes 0, filter takes 17904, total 1034621
	//takes 27180, storage takes 5998 of jet takes 0, filter takes 27175, total 1003618
	//takes 27489, storage takes 6171 of jet takes 0, filter takes 27480, total 1088484
	//takes 28922, storage takes 6563 of jet takes 0, filter takes 28920, total 1072802

	//takes 50084, storage takes 35832 of jet takes 23390, filter takes 50075, total 1003618
	//takes 50991, storage takes 36816 of jet takes 24163, filter takes 50873, total 1034621
	//takes 51858, storage takes 38346 of jet takes 25026, filter takes 51827, total 1088484
	//takes 51881, storage takes 31584 of jet takes 24815, filter takes 51880, total 1072802

	//takes 32893, storage takes 21539 of jet takes 16353, filter takes 32723, total 1004301
	//takes 33886, storage takes 22882 of jet takes 17607, filter takes 33837, total 1032305
	// takes 37312, storage takes 25897 of jet takes 20380, filter takes 37293, total 1104299
	//takes 37693, storage takes 25582 of jet takes 20082, filter takes 37628, total 1078070

	//takes 15306, storage takes 6054, filter takes 8456, total 144928
	//takes 19040, storage takes 6297, filter takes 15509, total 159559
	//takes 19980, storage takes 7025, filter takes 19938, total 181250
	//takes 20190, storage takes 13800, filter takes 20189, total 182995
	//takes 4965, storage takes 3350, filter takes 4949, total 153657
	//takes 4388, storage takes 2953, filter takes 4362, total 143891
	//takes 4974, storage takes 3380, filter takes 4955, total 165692
	//takes 15574, storage takes 8531, filter takes 15571, total 320062

	public JobContext handleFinishedPartition(JobContext jobContext) {
		PartitionTableOffset partitionTableOffset = (PartitionTableOffset) ((Map<?, ?>) sourcePdkDataNode.getSyncProgress().getBatchOffsetObj()).get(table);
		if (partitionTableOffset == null) {
			partitionTableOffset = new PartitionTableOffset();
			((Map<String, PartitionTableOffset>) sourcePdkDataNode.getSyncProgress().getBatchOffsetObj()).put(table, partitionTableOffset);
		}
		Map<String, Long> completedPartitions = partitionTableOffset.getCompletedPartitions();
		if (completedPartitions == null) {
			completedPartitions = new ConcurrentHashMap<>();
			completedPartitions.put(readPartition.getId(), sentEventCount.longValue());
			partitionTableOffset.setCompletedPartitions(completedPartitions);
		} else {
			completedPartitions.put(readPartition.getId(), sentEventCount.longValue());
		}
		sourcePdkDataNode.getObsLogger().info("Finished partition {} completedPartitions {}", readPartition, completedPartitions.size());
		return null;
	}

	public void passThrough(TapEvent event) {
		if (finished.get())
			sourcePdkDataNode.handleStreamEventsReceived(list(event), null);
	}

	private void passThrough0(TapEvent event) {
		sourcePdkDataNode.handleStreamEventsReceived(list(event), null);
	}

	public boolean isFinished() {
		return finished.get();
	}

	public void finish() {
		finished.set(true);
	}


	@Override
	public void handleUpdateRecordEvent(TapUpdateRecordEvent updateRecordEvent, Map<String, Object> after, Map<String, Object> key) {
		this.passThrough0(updateRecordEvent);
	}

	@Override
	public void handleInsertRecordEvent(TapInsertRecordEvent insertRecordEvent, Map<String, Object> after, Map<String, Object> key) {
		this.passThrough0(insertRecordEvent);
	}

	@Override
	public void handleDeleteRecordEvent(TapDeleteRecordEvent deleteRecordEvent, Map<String, Object> key) {
		this.passThrough0(deleteRecordEvent);
	}

	@Override
	public void deleteFromPartition(TapDeleteRecordEvent deleteRecordEvent, Map<String, Object> key) {
		//this.passThrough(deleteRecordEvent);
	}
}
