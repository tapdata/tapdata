package io.tapdata.websocket;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 防抖处理
 *
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/8/19 上午10:27
 */
public class DebounceUtil {

	private static Logger log = LoggerFactory.getLogger(DebounceUtil.class);

	private static Map<String, Job> wittingJobs = new ConcurrentHashMap<>();
	private static Map<String, ScheduledFuture<?>> submitJobs = new ConcurrentHashMap<>();
	private static ScheduledExecutorService executor =
			Executors.newScheduledThreadPool(20);

	/**
	 * 提交防抖待执行任务
	 *
	 * @param uniqueId  待执行任务唯一ID，需 保证全局唯一
	 * @param amplitude 抖动时间，耗秒
	 * @param runnable  待执行任务
	 */
	public static void debounce(String uniqueId, int amplitude, Runnable runnable) {

		String threadName = Thread.currentThread().getName();
		Job job = wittingJobs.remove(uniqueId);
		ScheduledFuture<?> future = submitJobs.remove(uniqueId);

		if (job != null) {
			job.setRunnable(runnable);
		} else {
			job = new Job(uniqueId, threadName, runnable);
		}

		if (future != null) {
			future.cancel(false);
		}

		future = executor.schedule(job, amplitude, TimeUnit.MILLISECONDS);

		wittingJobs.put(uniqueId, job);
		submitJobs.put(uniqueId, future);
	}

	/**
	 * 取消执行任务
	 *
	 * @param uniqueId              待执行任务唯一ID
	 * @param mayInterruptIfRunning 是否打断正在运行的任务
	 * @return
	 */
	public static Runnable cancel(String uniqueId, boolean mayInterruptIfRunning) {
		Job job = wittingJobs.remove(uniqueId);
		ScheduledFuture<?> future = submitJobs.remove(uniqueId);
		if (future != null) {
			future.cancel(mayInterruptIfRunning);
		}
		return job;
	}

	public static int wittingJobs() {
		return wittingJobs.size();
	}

	public static void shutdown() {
		executor.shutdownNow();
	}

	@RequiredArgsConstructor
	private static class Job implements Runnable {
		@Getter
		@NonNull
		private String id;
		@Getter
		@NonNull
		private String name;
		@Setter
		@NonNull
		private Runnable runnable;

		@Override
		public void run() {
			Job job = wittingJobs.remove(id);
			submitJobs.remove(id);
			if (job != null) {
				Thread.currentThread().setName(name);
				runnable.run();
			} else {
				log.warn("Job {} removed in debounceExec, canceled execute", name);
			}
		}
	}

}
