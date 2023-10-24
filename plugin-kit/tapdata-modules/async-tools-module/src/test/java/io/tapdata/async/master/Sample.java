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
		JobMaster asyncMaster = InstanceFactory.instance(JobMaster.class);


		ParallelWorker parallelWorker = asyncMaster.createAsyncParallelWorker("test", 2);
		parallelWorker.job("1", JobContext.create(""), asyncQueueWorker -> asyncQueueWorker.job("", jobContext -> null));
		parallelWorker.job("2", JobContext.create(""), asyncQueueWorker -> asyncQueueWorker.job("", jobContext -> null));
		parallelWorker.job("3", JobContext.create(""), asyncQueueWorker -> asyncQueueWorker.job("", jobContext -> null));
		parallelWorker.job("4", JobContext.create(""), asyncQueueWorker -> asyncQueueWorker.job("", jobContext -> null));
		parallelWorker.job("", JobContext.create(""), null);
		parallelWorker.job("", JobContext.create(""), null);
		parallelWorker.job("", JobContext.create(""), null);



		QueueWorker asyncQueueWorker = asyncMaster.createAsyncQueueWorker("");
		JobChain asyncJobChain = asyncMaster.createAsyncJobChain();
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
