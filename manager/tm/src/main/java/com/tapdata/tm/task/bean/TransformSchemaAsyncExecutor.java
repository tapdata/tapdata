package com.tapdata.tm.task.bean;

import com.tapdata.tm.utils.CommonUtil;
import com.tapdata.tm.utils.MockRequest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.util.concurrent.Executor;

@Configuration
@EnableAsync
public class TransformSchemaAsyncExecutor {
    public static final String TRANSFORM_SCHEMA_ASYNC_THREAD_NAME="transformSchemaAsyncThread";

    @Bean(name = TRANSFORM_SCHEMA_ASYNC_THREAD_NAME)
    public Executor asyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        executor.setCorePoolSize(30);
        executor.setMaxPoolSize(100);
        executor.setQueueCapacity(100);
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setThreadNamePrefix("AsyncThread-");
        executor.setTaskDecorator(runnable -> {
            ServletRequestAttributes servletRequestAttributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            HttpServletRequest request = servletRequestAttributes.getRequest();
            MockRequest mockRequest = new MockRequest();
            mockRequest.setQueryString(request.getQueryString());
            mockRequest.setAuthorization(request.getHeader("authorization"));
            mockRequest.setUserId(request.getHeader("user_id"));
            return () -> {
                try {
                    CommonUtil.shareRequest(mockRequest);
                    runnable.run();
                } finally {
                    CommonUtil.remove();
                }
            };
        });

        return executor;
    }
}
