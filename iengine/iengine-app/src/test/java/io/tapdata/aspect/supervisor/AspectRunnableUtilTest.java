package io.tapdata.aspect.supervisor;

import io.tapdata.aspect.supervisor.entity.DisposableThreadGroupBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/3/4 11:37 Create
 */
class AspectRunnableUtilTest {

	@Test
	void testAspectRunnable() {
		DisposableThreadGroupAspect<DisposableThreadGroupBase> aspect = Mockito.mock(DisposableThreadGroupAspect.class);
		Mockito.when(aspect.release()).thenReturn(aspect);

		CountDownLatch checkRunnableIsCalled = new CountDownLatch(1);
		Runnable realRunnable = checkRunnableIsCalled::countDown;

		Runnable aspectRunnable = AspectRunnableUtil.aspectRunnable(aspect, realRunnable);
		Assertions.assertNotNull(aspectRunnable);
		aspectRunnable.run();

		try {
			// 等待子线程被调用一次，最多等待5秒
			Assertions.assertTrue(checkRunnableIsCalled.await(5, TimeUnit.SECONDS), "Runnable is not called");
		} catch (InterruptedException ignore) {
			Thread.currentThread().interrupt();
		}

		// check call release
		Mockito.verify(aspect, Mockito.times(1)).release();
	}

	@Test
	void testAspectAndStart() {
		try (MockedStatic<AspectRunnableUtil> aspectRunnableUtilMockedStatic = Mockito.mockStatic(AspectRunnableUtil.class, Mockito.CALLS_REAL_METHODS)) {
			CountDownLatch checkRunnableIsCalled = new CountDownLatch(1);
			Runnable runFun = checkRunnableIsCalled::countDown;

			aspectRunnableUtilMockedStatic.when(() -> AspectRunnableUtil.aspectRunnable(Mockito.any(), Mockito.any())).thenReturn(runFun);
			AspectRunnableUtil.aspectAndStart(Mockito.any(), Mockito.any());

			try {
				// 等待子线程被调用一次，最多等待5秒
				Assertions.assertTrue(checkRunnableIsCalled.await(5, TimeUnit.SECONDS), "Runnable is not called");
			} catch (InterruptedException ignore) {
				Thread.currentThread().interrupt();
			}

		}
	}
}
