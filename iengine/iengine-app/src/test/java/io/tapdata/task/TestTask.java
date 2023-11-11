package io.tapdata.task;

import com.google.gson.Gson;
import com.tapdata.entity.ScheduleTask;
import org.junit.Assert;

/**
 * @author lg
 * Create by lg on 9/9/19 5:22 PM
 */
public class TestTask {

	/**
	 * test drop index
	 */
	// @Test
	public void testDropIndex() {

		ScheduleTask scheduleTask = new Gson().fromJson(
				"{ \"_id\" : \"5d760fd4bf32da126c69167c\", \"task_name\" : \"mongodb_drop_index\", \"task_type\" : \"MONGODB_DROP_INDEX\", \"status\" : \"waiting\", \"task_data\" : { \"collection_name\" : \"Workers\", \"uri\" : \"mongodb://127.0.0.1/tapdata\", \"name\" : \"ping_date_1\", \"key\" : { \"hostname\" : 1 }, \"status\" : \"creating\", \"create_by\" : \"admin@admin.com\" } }",
				ScheduleTask.class);
		TaskContext taskContext = new TaskContext(scheduleTask, null, null, null);
		MongodbDropIndexTask task = new MongodbDropIndexTask();
		task.initialize(taskContext);

		task.execute(taskResult -> {
			System.out.println(taskResult);
			Assert.assertEquals(taskResult.getTaskResultCode(), 200);
		});

	}

}
