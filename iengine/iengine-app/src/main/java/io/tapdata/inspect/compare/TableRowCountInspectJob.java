package io.tapdata.inspect.compare;

import com.tapdata.entity.inspect.InspectStatus;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.inspect.InspectJob;
import io.tapdata.inspect.InspectTaskContext;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * table rows inspect task
 *
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/10 8:34 上午
 * @description
 */
public class TableRowCountInspectJob extends InspectJob {
	private static final String TAG = TableRowCountInspectJob.class.getSimpleName();
	private Logger logger = LogManager.getLogger(TableRowCountInspectJob.class);

	public TableRowCountInspectJob(InspectTaskContext inspectTaskContext) {
		super(inspectTaskContext);
	}

	@Override
	protected void doRun() {
		try {
			int retry = 0;
			while (retry < 4) {
				try {
					AtomicLong sourceCount = new AtomicLong();
					AtomicLong targetCount = new AtomicLong();
					BatchCountFunction srcBatchCountFunction = this.sourceNode.getConnectorFunctions().getBatchCountFunction();
					if (null == srcBatchCountFunction) {
						retry = 3;
						throw new RuntimeException("Source node does not support batch count function: " + sourceNode.getConnectorContext().getSpecification().getId());
					}
					BatchCountFunction tgtBatchCountFunction = this.targetNode.getConnectorFunctions().getBatchCountFunction();
					if (null == tgtBatchCountFunction) {
						retry = 3;
						throw new RuntimeException("Target node does not support batch count function: " + targetNode.getConnectorContext().getSpecification().getId());
					}
					PDKInvocationMonitor.invoke(this.sourceNode, PDKMethod.SOURCE_BATCH_COUNT,
							() -> sourceCount.set(srcBatchCountFunction.count(this.sourceNode.getConnectorContext(), new TapTable(inspectTask.getSource().getTable()))),
							TAG
					);
					PDKInvocationMonitor.invoke(this.targetNode, PDKMethod.SOURCE_BATCH_COUNT,
							() -> targetCount.set(tgtBatchCountFunction.count(this.targetNode.getConnectorContext(), new TapTable(inspectTask.getTarget().getTable()))),
							TAG
					);

					boolean passed = sourceCount.get() == targetCount.get();

					stats.setEnd(new Date());
					stats.setStatus("done");
					stats.setResult(passed ? "passed" : "failed");
					stats.setProgress(1);
					stats.setSource_only(sourceCount.get());
					stats.setTarget_only(targetCount.get());
					stats.setSource_total(sourceCount.get());
					stats.setTarget_total(targetCount.get());
					break;
				} catch (Exception e) {
					if (retry >= 3) {
						logger.error(String.format("Failed to compare the count of rows in table %s.%s and table %s.%s, the taskId is %s",
								source.getName(), inspectTask.getSource().getTable(),
								target.getName(), inspectTask.getTarget().getTable(), inspectTask.getTaskId()), e);
						stats.setEnd(new Date());
						stats.setStatus(InspectStatus.ERROR.getCode());
						stats.setResult("failed");
						stats.setErrorMsg(e.getMessage() + "\n" +
								Arrays.stream(e.getStackTrace()).map(StackTraceElement::toString).collect(Collectors.joining("\n")));
						break;
					}
					retry++;
					stats.setErrorMsg(String.format("Check has an exception and is trying again..., The number of retries: %s", retry));
					stats.setStatus(InspectStatus.ERROR.getCode());
					stats.setEnd(new Date());
					stats.setResult("failed");
					progressUpdateCallback.progress(inspectTask, stats, null);
					logger.error(String.format("Check has an exception and is trying again..., The number of retries: %s", retry));
					try {
						TimeUnit.SECONDS.sleep(5);
					} catch (InterruptedException interruptedException) {
						break;
					}
				}
			}
		} catch (Throwable e) {
			logger.error("Inspect failed " + name, e);
		}
	}
}
