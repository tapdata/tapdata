package com.tapdata.tm.observability.service.impl;

import cn.hutool.extra.spring.SpringUtil;
import com.google.common.collect.Lists;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.observability.constant.BatchServiceEnum;
import com.tapdata.tm.observability.dto.BatchRequestDto;
import com.tapdata.tm.observability.dto.BatchUriParamDto;
import com.tapdata.tm.observability.service.ObservabilityService;
import com.tapdata.tm.observability.vo.BatchDataVo;
import com.tapdata.tm.observability.vo.BatchResponeVo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ObservabilityServiceImpl implements ObservabilityService {
    @Override
    public BatchResponeVo batch(BatchRequestDto batchRequestDto) throws ExecutionException, InterruptedException {

        List<CompletableFuture<BatchResponeVo>> futuresList  = Lists.newLinkedList();

        batchRequestDto.forEach((k, v) -> {
            BatchUriParamDto req = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(v), BatchUriParamDto.class);
            Map<?, ?> param = Objects.requireNonNull(req).getParam();
            String uri = req.getUri();

            BatchServiceEnum serviceEnum = BatchServiceEnum.getEnumByServiceAndMethod(uri);
            CompletableFuture<BatchResponeVo> query = CompletableFuture.supplyAsync(() -> {
                BatchResponeVo result = new BatchResponeVo();
                try {
                    if (Objects.isNull(serviceEnum)) {
                        result.put(k, new BatchDataVo("SystemError", "not required service method", null));
                        return result;
                    }

                    Class<?> serviceClass = Class.forName(serviceEnum.getService());
                    Class<?> paramClass = Class.forName(serviceEnum.getParam());
                    Method method = serviceClass.getMethod(serviceEnum.getMethod(), paramClass);

                    Object obj = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(param), paramClass);
                    Object bean = SpringUtil.getBean(serviceClass);
                    Object invoke = method.invoke(bean, obj);
                    result.put(k, new BatchDataVo("ok", null, invoke));
                    return result;
                } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException |
                         IllegalAccessException e) {
                    log.error("ObservabilityService batch method error msg:{}", e.getMessage(), e);
                    result.put(k, new BatchDataVo("SystemError", e.getCause().getMessage(), null));
                    return result;
                }
            });

            futuresList.add(query);
        });
        CompletableFuture<Void> allCompletableFuture = CompletableFuture.allOf(futuresList.toArray(new CompletableFuture[0]));

        BatchResponeVo result = new BatchResponeVo();
        allCompletableFuture.thenApply(v ->
                futuresList.stream().map(CompletableFuture::join).collect(Collectors.toList())).get().
                forEach(result::putAll);

        return result;
    }
}
