package io.tapdata.aspect;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.AspectTaskManager;
import io.tapdata.entity.aspect.AspectManager;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.utils.InstanceFactory;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.List;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.insertRecordEvent;
import static io.tapdata.entity.simplify.TapSimplify.map;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNotEquals;

public class AspectTest {
	@Test
	public void testTaskLifeCircle() {
		AspectTaskManager aspectTaskManager = InstanceFactory.instance(AspectTaskManager.class);
		assertNotNull(aspectTaskManager);
		AspectManager aspectManager = InstanceFactory.instance(AspectManager.class);
		assertNotNull(aspectManager);
		TaskDto taskDto = new TaskDto();
		taskDto.setId(new ObjectId());
		taskDto.setSyncType("TEST_TARGET");

		aspectManager.executeAspect(new TaskStartAspect().task(taskDto));
		List<AspectTask> aspectTasks = aspectTaskManager.getAspectTasks(taskDto.getId().toString());
		TestSampleTask testSampleTask = null;
		for (AspectTask aspectTask : aspectTasks) {
			if (aspectTask.getClass().equals(TestSampleTask.class)) {
				testSampleTask = (TestSampleTask) aspectTask;
				break;
			}
		}
		assertNotNull(testSampleTask);

		aspectManager.executeAspect(new TaskStartAspect().task(taskDto));
		aspectTasks = aspectTaskManager.getAspectTasks(taskDto.getId().toString());
		TestSampleTask testSampleTaskAnother = null;
		for (AspectTask aspectTask : aspectTasks) {
			if (aspectTask.getClass().equals(TestSampleTask.class)) {
				testSampleTaskAnother = (TestSampleTask) aspectTask;
				break;
			}
		}
		assertNotNull(testSampleTaskAnother);
		assertNotEquals(testSampleTaskAnother, testSampleTask);
		assertEquals(1, testSampleTask.onStartCounter.intValue());
		assertEquals(1, testSampleTask.onStopCounter.intValue());

		// the old task has been removed, use the new task to replace the old one;
		testSampleTask = testSampleTaskAnother;

		ProcessorBaseContext processorBaseContext = ProcessorBaseContext.newBuilder().withTaskDto(taskDto).build();

		TapdataEvent event = new TapdataEvent();
		event.setTapEvent(insertRecordEvent(map(entry("aa", 1)), "table1"));
		ProcessorNodeProcessAspect processorNodeProcessAspect = new ProcessorNodeProcessAspect().processorBaseContext(processorBaseContext).state(ProcessorFunctionAspect.STATE_START).inputEvent(event);
		aspectManager.executeAspect(processorNodeProcessAspect);
		assertNotNull(testSampleTask.nodeProcessAspect);
		assertEquals("table1", ((TapInsertRecordEvent) testSampleTask.nodeProcessAspect.getInputEvent().getTapEvent()).getTableId());
		assertEquals(ProcessorNodeProcessAspect.STATE_START, testSampleTask.nodeProcessAspect.getState());

		aspectManager.executeAspect(processorNodeProcessAspect.state(ProcessorFunctionAspect.STATE_END));
		assertEquals(ProcessorNodeProcessAspect.STATE_END, testSampleTask.nodeProcessAspect.getState());

		aspectManager.executeAspect(new TaskStopAspect().task(taskDto));
		aspectTasks = aspectTaskManager.getAspectTasks(taskDto.getId().toString());
		TestSampleTask testSampleTaskNone = null;
		for (AspectTask aspectTask : aspectTasks) {
			if (aspectTask.getClass().equals(TestSampleTask.class)) {
				testSampleTaskNone = (TestSampleTask) aspectTask;
				break;
			}
		}
		assertNull(testSampleTaskNone);
		assertEquals(1, testSampleTask.onStartCounter.intValue());
		assertEquals(1, testSampleTask.onStopCounter.intValue());

		aspectManager.executeAspect(new TaskStopAspect().task(taskDto));
		testSampleTaskNone = null;
		for (AspectTask aspectTask : aspectTasks) {
			if (aspectTask.getClass().equals(TestSampleTask.class)) {
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
		TaskDto taskDto = new TaskDto();
		taskDto.setId(new ObjectId());
		taskDto.setSyncType("include1");
		aspectManager.executeAspect(new TaskStartAspect().task(taskDto));

		List<AspectTask> taskList = aspectTaskManager.getAspectTasks(taskDto.getId().toString());
		boolean hasIncludesSampleTask = false;
		boolean hasDefaultSampleTask = false;
		for (AspectTask aspectTask : taskList) {
			if (aspectTask.getClass().equals(IncludesSampleTask.class)) {
				hasIncludesSampleTask = true;
			}
			if (aspectTask.getClass().equals(DefaultSampleTask.class)) {
				hasDefaultSampleTask = true;
			}
		}
		assertTrue(hasIncludesSampleTask);
		assertTrue(hasDefaultSampleTask);
		aspectManager.executeAspect(new TaskStopAspect().task(taskDto));

		taskList = aspectTaskManager.getAspectTasks(taskDto.getId().toString());
		boolean hasIncludesSampleTask1 = false;
		boolean hasDefaultSampleTask1 = false;
		for (AspectTask aspectTask : taskList) {
			if (aspectTask.getClass().equals(IncludesSampleTask.class)) {
				hasIncludesSampleTask1 = true;
			}
			if (aspectTask.getClass().equals(DefaultSampleTask.class)) {
				hasDefaultSampleTask1 = true;
			}
		}
		assertFalse(hasIncludesSampleTask1);
		assertFalse(hasDefaultSampleTask1);
	}

	@Test
	public void testMultiTasks() {
		AspectTaskManager aspectTaskManager = InstanceFactory.instance(AspectTaskManager.class);
		assertNotNull(aspectTaskManager);
		AspectManager aspectManager = InstanceFactory.instance(AspectManager.class);
		assertNotNull(aspectManager);
		TaskDto taskDto = new TaskDto();
		taskDto.setName("A");
		taskDto.setId(new ObjectId());
		taskDto.setSyncType("TEST_TARGET");

		TaskDto taskDto1 = new TaskDto();
		taskDto1.setId(new ObjectId());
		taskDto1.setName("B");
		taskDto1.setSyncType("TEST_TARGET");

		aspectManager.executeAspect(new TaskStartAspect().task(taskDto));
		aspectManager.executeAspect(new TaskStartAspect().task(taskDto1));
		List<AspectTask> aspectTasks = aspectTaskManager.getAspectTasks(taskDto.getId().toString());
		TestSampleTask testSampleTask = null;
		for (AspectTask aspectTask : aspectTasks) {
			if (aspectTask.getClass().equals(TestSampleTask.class)) {
				testSampleTask = (TestSampleTask) aspectTask;
				break;
			}
		}
		assertNotNull(testSampleTask);

		aspectTasks = aspectTaskManager.getAspectTasks(taskDto1.getId().toString());
		TestSampleTask testSampleTask1 = null;
		for (AspectTask aspectTask : aspectTasks) {
			if (aspectTask.getClass().equals(TestSampleTask.class)) {
				testSampleTask1 = (TestSampleTask) aspectTask;
				break;
			}
		}
		assertNotNull(testSampleTask1);

		ProcessorBaseContext processorBaseContext = ProcessorBaseContext.newBuilder().withTaskDto(taskDto).build();

		TapdataEvent event = new TapdataEvent();
		event.setTapEvent(insertRecordEvent(map(entry("aa", 1)), "table1"));
		ProcessorNodeProcessAspect processorNodeProcessAspect = new ProcessorNodeProcessAspect().processorBaseContext(processorBaseContext).state(ProcessorFunctionAspect.STATE_START).inputEvent(event);
		aspectManager.executeAspect(processorNodeProcessAspect);
		assertNotNull(testSampleTask.nodeProcessAspect);
		assertEquals("table1", ((TapInsertRecordEvent) testSampleTask.nodeProcessAspect.getInputEvent().getTapEvent()).getTableId());
		assertEquals(ProcessorNodeProcessAspect.STATE_START, testSampleTask.nodeProcessAspect.getState());

		aspectManager.executeAspect(processorNodeProcessAspect.state(ProcessorFunctionAspect.STATE_END));
		assertEquals(ProcessorNodeProcessAspect.STATE_END, testSampleTask.nodeProcessAspect.getState());

		aspectManager.executeAspect(new TaskStopAspect().task(taskDto));
		aspectManager.executeAspect(new TaskStopAspect().task(taskDto1));
		aspectTasks = aspectTaskManager.getAspectTasks(taskDto.getId().toString());
		TestSampleTask testSampleTaskNone = null;
		for (AspectTask aspectTask : aspectTasks) {
			if (aspectTask.getClass().equals(TestSampleTask.class)) {
				testSampleTaskNone = (TestSampleTask) aspectTask;
				break;
			}
		}
		assertNull(testSampleTaskNone);
		assertEquals(1, testSampleTask.onStartCounter.intValue());
		assertEquals(1, testSampleTask.onStopCounter.intValue());

		aspectTasks = aspectTaskManager.getAspectTasks(taskDto1.getId().toString());
		TestSampleTask testSampleTaskNone1 = null;
		for (AspectTask aspectTask : aspectTasks) {
			if (aspectTask.getClass().equals(TestSampleTask.class)) {
				testSampleTaskNone1 = (TestSampleTask) aspectTask;
				break;
			}
		}
		assertNull(testSampleTaskNone1);
		assertEquals(1, testSampleTask1.onStartCounter.intValue());
		assertEquals(1, testSampleTask1.onStopCounter.intValue());
	}

	public static void main(String[] args) {
		AspectManager aspectManager = InstanceFactory.instance(AspectManager.class);
		for (int i = 0; i < 100; i++)
			aspectManager.executeAspect(new TaskResetAspect());

//        aspectManager.registerAspectObserver(EmptyAspect.class, 1, new AspectObserver<EmptyAspect>() {
//            @Override
//            public void observe(EmptyAspect aspect) {
//
//            }
//        });

		long time = System.currentTimeMillis();
		for (int i = 0; i < 1000000; i++)
			aspectManager.executeAspect(new TaskResetAspect());
		System.out.println("takes " + (System.currentTimeMillis() - time));
		//takes 57 if it has one observer
		//takes 24 if it has none observer



		time = System.currentTimeMillis();
		for (int i = 0; i < 1000000; i++)
			aspectManager.executeAspect(TaskResetAspect.class, TaskResetAspect::new);
		System.out.println("callable takes " + (System.currentTimeMillis() - time));
		//callable takes 48 if it has one observer
		//callable takes 9 if it has none observer
	}
}
