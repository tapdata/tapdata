package com.tapdata.tm.monitor.service.impl;

import cn.hutool.core.date.StopWatch;
import cn.hutool.extra.spring.SpringUtil;
import com.google.common.collect.Lists;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.monitor.constant.BatchServiceEnum;
import com.tapdata.tm.monitor.dto.BatchRequestDto;
import com.tapdata.tm.monitor.dto.BatchUriParamDto;
import com.tapdata.tm.monitor.service.BatchService;
import com.tapdata.tm.monitor.vo.BatchDataVo;
import com.tapdata.tm.monitor.vo.BatchResponeVo;
import io.tapdata.common.executor.ExecutorsManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoUnit.SECONDS;

@Service
@Slf4j
public class BatchServiceImpl implements BatchService {

    private static final ScheduledExecutorService scheduler = ExecutorsManager.getInstance().getScheduledExecutorService();

    @Override
    public BatchResponeVo batch(BatchRequestDto batchRequestDto) throws ExecutionException, InterruptedException {

        List<CompletableFuture<BatchResponeVo>> futuresList = Lists.newLinkedList();

        batchRequestDto.forEach((k, v) -> {
            BatchUriParamDto req = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(v), BatchUriParamDto.class);
            Map<?, ?> param = Objects.requireNonNull(req).getParam();
            String uri = req.getUri();

            BatchServiceEnum serviceEnum = BatchServiceEnum.getEnumByServiceAndMethod(uri);
            CompletableFuture<BatchResponeVo> query = CompletableFuture.supplyAsync(() -> {
                BatchResponeVo result = new BatchResponeVo();
                try {
                    if (Objects.isNull(serviceEnum)) {
                        result.put(k, new BatchDataVo("SystemError", "not required service method", null, null));
                        return result;
                    }
                    StopWatch stopWatch = new StopWatch();
                    stopWatch.start();
                    Class<?> serviceClass = Class.forName(serviceEnum.getService());
                    Class<?> paramClass = Class.forName(serviceEnum.getParam());
                    Method method = serviceClass.getMethod(serviceEnum.getMethod(), paramClass);

                    Object obj = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(param), paramClass);
                    Object bean = SpringUtil.getBean(serviceClass);
                    Object data = method.invoke(bean, obj);
                    stopWatch.stop();
                    result.put(k, new BatchDataVo("ok", null, data, stopWatch.getTotalTimeMillis()));

                    return result;
                } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException |
                         IllegalAccessException e) {
                    log.error("BatchService batch method error msg:{}", e.getMessage(), e);
                    result.put(k, new BatchDataVo("SystemError", e.getCause().getMessage(), null, null));
                    return result;
                }
            }, scheduler);

            final CompletableFuture<BatchResponeVo> chains = within(query, Duration.ofSeconds(30), k);
            futuresList.add(chains);
        });
        CompletableFuture<Void> allCompletableFuture = CompletableFuture.allOf(futuresList.toArray(new CompletableFuture[0]));

        BatchResponeVo result = new BatchResponeVo();
        allCompletableFuture.thenApply(v -> futuresList.stream().map(CompletableFuture::join).collect(Collectors.toList()))
                .get()
                .forEach(result::putAll);
        return result;
    }

    public CompletableFuture<BatchResponeVo> failAfter(Duration duration, String key){
        /// need a schedular executor
        final CompletableFuture<BatchResponeVo> timer = new CompletableFuture<>();
        scheduler.schedule(()-> timer.complete(new BatchResponeVo() {{
            put(key, new BatchDataVo("SystemError", "method excute timeout "+duration.get(SECONDS)+"s", null, null));
        }}),duration.toMillis(), TimeUnit.MILLISECONDS);
        return timer;
    }

    public CompletableFuture<BatchResponeVo> within(CompletableFuture<BatchResponeVo> taskFuture, Duration duration, String key){
        CompletableFuture<BatchResponeVo> timeoutWatcher = failAfter(duration, key);
        return taskFuture.applyToEither(timeoutWatcher, Function.identity());
    }
}
