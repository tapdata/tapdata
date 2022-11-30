package io.tapdata.async.master;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.InstanceFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author aplomb
 */
public class Sample {
	public static void main(String[] args) {
		AsyncMaster asyncMaster = InstanceFactory.instance(AsyncMaster.class);


		AsyncParallelWorker parallelWorker = asyncMaster.createAsyncParallelWorker("", 1);
		parallelWorker.start("", JobContext.create(""), null);
		parallelWorker.start("", JobContext.create(""), null);
		parallelWorker.start("", JobContext.create(""), null);
		parallelWorker.start("", JobContext.create(""), null);



		AsyncQueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("");
		AsyncJobChain asyncJobChain = asyncMaster.createAsyncJobChain();
		asyncJobChain.externalJob("batchRead", jobContext -> {
			return jobContext;
		}).job("batchRead1", (jobContext) -> {
			String id = jobContext.getId();
			List<TapEvent> eventList = (List<TapEvent>) jobContext.getResult();
			//batch read
//			List<TapEvent> eventList = new ArrayList<>();
			jobContext.foreach(eventList, event -> {

				return null;
			});
			Map<String, TapField> fieldMap = new HashMap<>();
			jobContext.foreach(fieldMap, stringTapFieldEntry -> {
				stringTapFieldEntry.getKey();
				return null;
			});
			jobContext.runOnce(() -> {
				eventList.add(null);
			});
			if(true)
				return JobContext.create("").jumpToId("streamRead");
			else
				return JobContext.create("");
		}).job("streamRead", (jobContext) -> {
			Object o = jobContext.getResult();
			//stream read
			return JobContext.create("");
		});

		asyncQueueWorker.job(asyncJobChain);
		asyncQueueWorker.cancelAll().job("doSomeThing", (jobContext) -> {
			//Do something.
			return null;
		}).job(asyncJobChain);
		asyncQueueWorker.start(JobContext.create("").context(new Object()));
	}
}
