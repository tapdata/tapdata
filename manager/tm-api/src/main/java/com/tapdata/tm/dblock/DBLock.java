package com.tapdata.tm.dblock;

import com.tapdata.tm.dblock.impl.StandardLock;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 数据库锁-入口类
 * <p>
 * 该类提供了创建数据库锁实例的功能，并维护了一个全局的线程池用于执行定时任务（如心跳检测）。
 * 它还定义了一些常量和工具方法，方便在分布式环境中进行锁管理。
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/8 11:55 Create
 */
@Slf4j
public class DBLock {
		/**
		 * 日志标签，用于标识日志来源
		 */
		public static final String TAG = DBLock.class.getSimpleName();

		/**
		 * 锁拥有者的默认值，表示没有被任何服务实例持有
		 */
		public static final String NONE_OWNER = "";

		/**
		 * 锁过期时间的默认值，表示初始状态（1970-01-01 00:00:00）
		 */
		public static final Date NONE_EXPIRE = new Date(0L);

		/**
		 * 线程组，用于组织与数据库锁相关的所有线程
		 */
		private static final ThreadGroup threadGroup = new ThreadGroup(TAG);

		/**
		 * 原子整数，用于生成唯一的线程ID
		 */
		private static final AtomicInteger idAtomic = new AtomicInteger(0);

		/**
		 * 全局调度线程池，用于执行定时任务（如心跳检测）
		 */
		public static final ScheduledExecutorService executor = initExecutorService(
						Math.max(Runtime.getRuntime().availableProcessors() * 2, 8),
						TimeUnit.SECONDS.toMillis(60L)
		);

		/**
		 * 工具方法：为日志或其他用途生成带前缀的字符串
		 *
		 * @param format 格式化字符串
		 * @param args   参数列表
		 * @return 带有类名前缀的格式化字符串
		 */
		public static String prefixTag(String format, Object... args) {
				return TAG + String.format(format, args);
		}

		/**
		 * 创建一个新的数据库锁实例
		 *
		 * @param repository 锁信息存储库，负责与数据库交互
		 * @param key        锁的唯一标识符
		 * @return 返回一个新的 StandardLock 实例
		 */
		public static ILock create(DBLockRepository repository, String key) {
				return new StandardLock(repository, key);
		}

		protected static ScheduledExecutorService initExecutorService(int corePoolSize, long keepAliveTime) {
				// 创建线程工厂，为每个线程设置唯一的线程名称
				ThreadFactory threadFactory = r -> {
						String threadName = prefixTag("-%d", idAtomic.getAndIncrement());
						return new Thread(threadGroup, r, threadName);
				};

				ScheduledThreadPoolExecutor instance = new ScheduledThreadPoolExecutor(corePoolSize, threadFactory);
//        // 允许核心线程超时销毁（解决空闲线程堆积）
				instance.allowCoreThreadTimeOut(true);
//        // 设置空闲线程存活时间
				instance.setKeepAliveTime(keepAliveTime, TimeUnit.MILLISECONDS);
				return instance;
		}
}
