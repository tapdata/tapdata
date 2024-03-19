package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.entity.TapdataShareLogEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import io.tapdata.entity.TapProcessorNodeContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.exception.TapCodeException;
import io.tapdata.node.pdk.processor.TapProcessorNode;
import io.tapdata.node.pdk.processor.TapTargetShareCDCNode;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.jetbrains.annotations.NotNull;
import java.util.*;

/**
 * @author samuel
 * @Description
 * @create 2022-06-14 17:23
 **/
public class HazelcastTargetPdkShareCDCNode extends HazelcastTargetPdkBaseNode {

	public static final String TAG = HazelcastTargetPdkShareCDCNode.class.getSimpleName();
	private TapProcessorNode processorNode;
	public HazelcastTargetPdkShareCDCNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		this.processorNode=new TapTargetShareCDCNode(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws TapCodeException {
		super.doInit(context);
		TapProcessorNodeContext tapProcessorNodeContext = initTapProcessorNodeContext();
		this.targetBatch = 10000;
		this.targetBatchIntervalMs = 1000;
		this.processorNode.doInit(context,tapProcessorNodeContext);
	}

	private TapProcessorNodeContext initTapProcessorNodeContext() {
		return TapProcessorNodeContext.builder()
				.codecsFilterManager(codecsFilterManager)
				.running(running)
				.externalStorageDto(externalStorageDto)
				.obsLogger(obsLogger)
				.jetContext(jetContext)
				.build();
	}

	@Override
	void processEvents(List<TapEvent> tapEvents) {
		throw new UnsupportedOperationException();
	}

	@Override
	void processShareLog(List<TapdataShareLogEvent> tapdataShareLogEvents) {
		this.processorNode.processShareLog(tapdataShareLogEvents);
	}


	@Override
	public void doClose() throws TapCodeException {
		CommonUtils.ignoreAnyError(()-> this.processorNode.doClose(), TAG);
		super.doClose();
	}
}
