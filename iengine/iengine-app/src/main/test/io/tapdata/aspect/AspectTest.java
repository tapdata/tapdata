package io.tapdata.aspect;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.ApplicationStartAspect;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.AspectTaskManager;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectManager;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.utils.InstanceFactory;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.reflections.Reflections;

import java.util.List;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static junit.framework.TestCase.*;

public class AspectTest {
	@Test
	public void testTaskLifeCircle() {
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

		TapdataEvent event = new TapdataEvent();
		event.setTapEvent(insertRecordEvent(map(entry("aa", 1)), "table1"));
		ProcessorNodeProcessAspect processorNodeProcessAspect = new ProcessorNodeProcessAspect().state(ProcessorFunctionAspect.STATE_START).inputEvent(event);
		aspectManager.executeAspect(processorNodeProcessAspect);
		assertNotNull(testSampleTask.nodeProcessAspect);
		assertEquals("table1", ((TapInsertRecordEvent)testSampleTask.nodeProcessAspect.getInputEvent().getTapEvent()).getTableId());
		assertEquals(ProcessorNodeProcessAspect.STATE_START, testSampleTask.nodeProcessAspect.getState());

		aspectManager.executeAspect(processorNodeProcessAspect.state(ProcessorFunctionAspect.STATE_END));
		assertEquals(ProcessorNodeProcessAspect.STATE_END, testSampleTask.nodeProcessAspect.getState());

		aspectManager.executeAspect(new TaskStopAspect().task(subTaskDto));
		aspectTasks = aspectTaskManager.getAspectTasks(subTaskDto.getId().toString());
		TestSampleTask testSampleTaskNone = null;
		for(AspectTask aspectTask : aspectTasks) {
			if(aspectTask.getClass().equals(TestSampleTask.class)) {
				testSampleTaskNone = (TestSampleTask) aspectTask;
				break;
			}
		}
		assertNull(testSampleTaskNone);
		assertEquals(1, testSampleTask.onStartCounter.intValue());
		assertEquals(1, testSampleTask.onStopCounter.intValue());

		aspectManager.executeAspect(new TaskStopAspect().task(subTaskDto));
		testSampleTaskNone = null;
		for(AspectTask aspectTask : aspectTasks) {
			if(aspectTask.getClass().equals(TestSampleTask.class)) {
				testSampleTaskNone = (TestSampleTask) aspectTask;
				break;
			}
		}
		assertNull(testSampleTaskNone);
		assertEquals(1, testSampleTask.onStartCounter.intValue());
		assertEquals(1, testSampleTask.onStopCounter.intValue());
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

	public static void main(String[] args) {
		AspectManager aspectManager = InstanceFactory.instance(AspectManager.class);
		for(int i = 0; i < 100; i++)
			aspectManager.executeAspect(new TaskResetAspect());

//        aspectManager.registerAspectObserver(EmptyAspect.class, 1, new AspectObserver<EmptyAspect>() {
//            @Override
//            public void observe(EmptyAspect aspect) {
//
//            }
//        });

		long time = System.currentTimeMillis();
		for(int i = 0; i < 1000000; i++)
			aspectManager.executeAspect(new TaskResetAspect());
		System.out.println("takes " + (System.currentTimeMillis() - time));
		//takes 57 if it has one observer
		//takes 24 if it has none observer



		time = System.currentTimeMillis();
		for(int i = 0; i < 1000000; i++)
			aspectManager.executeAspect(TaskResetAspect.class, TaskResetAspect::new);
		System.out.println("callable takes " + (System.currentTimeMillis() - time));
		//callable takes 48 if it has one observer
		//callable takes 9 if it has none observer
	}
}
