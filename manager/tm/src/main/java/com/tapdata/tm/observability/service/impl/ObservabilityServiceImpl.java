package com.tapdata.tm.observability.service.impl;

import cn.hutool.extra.spring.SpringUtil;
import com.google.common.collect.Lists;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.observability.constant.BatchServiceEnum;
import com.tapdata.tm.observability.dto.ServiceMethodDto;
import com.tapdata.tm.observability.service.ObservabilityService;
import com.tapdata.tm.observability.vo.ParallelInfoVO;
import org.springframework.stereotype.Service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
public class ObservabilityServiceImpl implements ObservabilityService {
    @Override
    public List<ParallelInfoVO<?>> getList(List<ServiceMethodDto<?>> serviceMethodDto) {

        List<CompletableFuture<ParallelInfoVO<?>>> futuresList  = Lists.newLinkedList();
        serviceMethodDto.forEach(rep -> {
            String serviceName = rep.getServiceName();
            String methodName = rep.getMethodName();
            Object param = rep.getParam();

            BatchServiceEnum serviceEnum = BatchServiceEnum.getEnumByServiceAndMethod(serviceName, methodName);
            if (Objects.isNull(serviceEnum)) {
                throw new BizException("not required service method");
            }

            CompletableFuture<ParallelInfoVO<?>> query = CompletableFuture.supplyAsync(() -> {
                try {
                    Method method = serviceEnum.getService().getMethod(methodName, serviceEnum.getParam().getClasses());

                    Object obj = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(param), serviceEnum.getParam());
                    Object invoke = method.invoke(serviceEnum.getResult(), obj);
                    return new ParallelInfoVO<>("ok", null, invoke);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            });

            futuresList.add(query);
        });
        CompletableFuture<Void> allCompletableFuture = CompletableFuture.allOf(futuresList.toArray(new CompletableFuture[futuresList.size()]));
        return allCompletableFuture.thenApply(e -> futuresList.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.<ParallelInfoVO<?>>toList())).join();
    }
}
