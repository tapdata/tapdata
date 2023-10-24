package io.tapdata.pdk.core.utils.test;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.error.QuiteException;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author aplomb
 */
public class AsyncTestBase {

	private static final String TAG = AsyncTestBase.class.getSimpleName();
	private final AtomicBoolean completed = new AtomicBoolean(false);
	private boolean finishSuccessfully = false;
	private Throwable lastThrowable;
	private AtomicBoolean enterWaitCompletedStage = new AtomicBoolean(false);
	public void waitCompleted(long seconds) throws Throwable {
		while (!completed.get()) {
			synchronized (completed) {
				enterWaitCompletedStage.compareAndSet(false, true);
				if (!completed.get()) {
					try {
						completed.wait(seconds * 1000);
						completed.set(true);
						if (lastThrowable == null && !finishSuccessfully)
							throw new TimeoutException("Waited " + seconds + " seconds and still not completed, consider timeout execution.");
					} catch (InterruptedException interruptedException) {
						interruptedException.printStackTrace();
						TapLogger.error(TAG, "Completed wait interrupted " + interruptedException.getMessage());
						Thread.currentThread().interrupt();
					}
				}
			}
		}
		try {
			if (lastThrowable != null)
				throw lastThrowable;
		} finally {
			tearDown();
			synchronized (this) {
				this.notifyAll();
			}
		}
	}

	public void tearDown() {

	}

	public interface AssertionCall {
		void assertIt() throws InvocationTargetException, IllegalAccessException;
	}

	public void $(AssertionCall assertionCall) {
		try {
			assertionCall.assertIt();
		} catch (Throwable throwable) {
			lastThrowable = throwable;
			completed(true);
		}
	}

	public void completed() {
		completed(false);
	}

	public void completed(boolean withError) {
		if (completed.compareAndSet(false, true)) {
			finishSuccessfully = !withError;
//            PDKLogger.enable(false);
			synchronized (completed) {
				completed.notifyAll();
			}
			if(withError) {
				synchronized (this) {
					try {
						this.wait(50000);
					} catch (InterruptedException interruptedException) {
						interruptedException.printStackTrace();
					}
					throw new QuiteException("Exit on failed");
				}
			}
		}
	}
}
