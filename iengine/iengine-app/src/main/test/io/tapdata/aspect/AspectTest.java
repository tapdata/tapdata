package io.tapdata.aspect;

import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.ApplicationStartAspect;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.AspectTaskManager;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectManager;
import io.tapdata.entity.utils.InstanceFactory;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.reflections.Reflections;

import java.util.List;

import static junit.framework.TestCase.*;

public class AspectTest {
	@Test
	public void a() {
		AspectTaskManager aspectTaskManager = InstanceFactory.instance(AspectTaskManager.class);
		assertNotNull(aspectTaskManager);
		AspectManager aspectManager = InstanceFactory.instance(AspectManager.class);
		assertNotNull(aspectManager);
		SubTaskDto subTaskDto = new SubTaskDto();
		subTaskDto.setId(new ObjectId());
		TaskDto taskDto = new TaskDto();
		taskDto.setSyncType("TEST_TARGET");
		subTaskDto.setParentTask(taskDto);

		aspectManager.executeAspect(new TaskStartAspect().task(subTaskDto));
		List<AspectTask> aspectTasks = aspectTaskManager.getAspectTasks(subTaskDto.getId().toString());
		TestSampleTask testSampleTask = null;
		for(AspectTask aspectTask : aspectTasks) {
			if(aspectTask.getClass().equals(TestSampleTask.class)) {
				testSampleTask = (TestSampleTask) aspectTask;
				break;
			}
		}
		assertNotNull(testSampleTask);

		aspectManager.executeAspect(new TaskStartAspect().task(subTaskDto));
		TestSampleTask testSampleTaskAnother = null;
		for(AspectTask aspectTask : aspectTasks) {
			if(aspectTask.getClass().equals(TestSampleTask.class)) {
				testSampleTaskAnother = (TestSampleTask) aspectTask;
				break;
			}
		}
		assertNotNull(testSampleTaskAnother);
		assertEquals(testSampleTaskAnother, testSampleTask);
		assertEquals(1, testSampleTask.onStartCounter.intValue());
		assertEquals(0, testSampleTask.onStopCounter.intValue());

		aspectManager.executeAspect(new ApplicationStartAspect());
		aspectManager.executeAspect(new TaskStopAspect().task(subTaskDto));

		aspectManager.executeAspect(new TaskStopAspect().task(subTaskDto));
	}

	@Test
	public void testIncludes() {
		AspectTaskManager aspectTaskManager = InstanceFactory.instance(AspectTaskManager.class);
		assertNotNull(aspectTaskManager);
		AspectManager aspectManager = InstanceFactory.instance(AspectManager.class);
		assertNotNull(aspectManager);
		SubTaskDto subTaskDto = new SubTaskDto();
		subTaskDto.setId(new ObjectId());
		TaskDto taskDto = new TaskDto();
		taskDto.setSyncType("include1");
		subTaskDto.setParentTask(taskDto);
		aspectManager.executeAspect(new TaskStartAspect().task(subTaskDto));

		List<AspectTask> taskList = aspectTaskManager.getAspectTasks(subTaskDto.getId().toString());
		boolean hasIncludesSampleTask = false;
		boolean hasDefaultSampleTask = false;
		for(AspectTask aspectTask : taskList) {
			if(aspectTask.getClass().equals(IncludesSampleTask.class)) {
				hasIncludesSampleTask = true;
			}
			if(aspectTask.getClass().equals(DefaultSampleTask.class)) {
				hasDefaultSampleTask = true;
			}
		}
		assertTrue(hasIncludesSampleTask);
		assertTrue(hasDefaultSampleTask);
		aspectManager.executeAspect(new TaskStopAspect().task(subTaskDto));

		taskList = aspectTaskManager.getAspectTasks(subTaskDto.getId().toString());
		boolean hasIncludesSampleTask1 = false;
		boolean hasDefaultSampleTask1 = false;
		for(AspectTask aspectTask : taskList) {
			if(aspectTask.getClass().equals(IncludesSampleTask.class)) {
				hasIncludesSampleTask1 = true;
			}
			if(aspectTask.getClass().equals(DefaultSampleTask.class)) {
				hasDefaultSampleTask1 = true;
			}
		}
		assertFalse(hasIncludesSampleTask1);
		assertFalse(hasDefaultSampleTask1);
	}
}
