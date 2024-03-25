package io.tapdata.milestone;

import com.tapdata.entity.ResponseBody;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.entity.logger.Log;
import io.tapdata.exception.TmUnavailableException;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/3/11 20:55 Create
 */
class MilestoneAspectTaskTest {

	void setField(MilestoneAspectTask milestoneAspectTask, Class<?> clz, String field, Object value) {
		try {
			Field logField = clz.getDeclaredField(field);
			logField.setAccessible(true);
			logField.set(milestoneAspectTask, value);
		} catch (Exception e) {
			throw new RuntimeException("Can't mock '" + field + "' field in " + MilestoneAspectTask.class.getSimpleName(), e);
		}
	}

	@Test
	void testTmUnavailableException() {
		try (MockedStatic<TmUnavailableException> tmUnavailableExceptionMockedStatic = Mockito.mockStatic(TmUnavailableException.class, Mockito.CALLS_REAL_METHODS)) {
			TmUnavailableException ex = new TmUnavailableException("test-url", "post", null, new ResponseBody());
			tmUnavailableExceptionMockedStatic.when(() -> TmUnavailableException.notInstance(ex)).thenReturn(true, false);

			TaskDto task = new TaskDto() {
				@Override
				public String getType() {
					throw ex;
				}
			};
			task.setId(new ObjectId());
			task.setName("test-task");

			MilestoneAspectTask milestoneAspectTask = Mockito.mock(MilestoneAspectTask.class, Mockito.CALLS_REAL_METHODS);
			setField(milestoneAspectTask, AspectTask.class, "log", Mockito.mock(Log.class));
			setField(milestoneAspectTask, MilestoneAspectTask.class, "executorService", Mockito.mock(ScheduledExecutorService.class));
			milestoneAspectTask.setTask(task);

			milestoneAspectTask.onStop(null); // available
			milestoneAspectTask.onStop(null); // unavailable
		}

	}
}
