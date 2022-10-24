package com.tapdata.tm.config;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.mongo.MongoLockProvider;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/11/11 10:08 上午
 * @description
 */
@Configuration
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "10m")
public class ScheduleConfig {

	@Bean
	public TaskScheduler taskScheduler() {
		final ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
		taskScheduler.setPoolSize(40);
		return taskScheduler;
	}

	@Bean
	public LockProvider getLockProvider(MongoTemplate mongoTemplate) {
		return new MongoLockProvider(mongoTemplate.getCollection("DrsScheduleLock"));
	}

	@Bean("NotificationExecutor")
	public Executor initNotificationExecutor () {
		int poolSize = Runtime.getRuntime().availableProcessors() * 2;
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(poolSize);
		executor.setMaxPoolSize(Math.max(poolSize, 50));
		executor.setQueueCapacity(200);
		executor.setKeepAliveSeconds(60);
		executor.setThreadNamePrefix("NotificationExecutor-");
		executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		executor.setWaitForTasksToCompleteOnShutdown(true);
		executor.setAwaitTerminationSeconds(600);
		return executor;
	}

}
