package com.tapdata.tm.group.handler;

import com.tapdata.tm.group.dto.ResourceType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 资源处理器注册中心
 * 负责资源处理器的注册和获取
 * 通过 Spring 自动注入所有 ResourceHandler 实现
 *
 */
@Slf4j
@Component
public class ResourceHandlerRegistry {

    /**
     * 资源类型到处理器的映射
     */
    private final Map<ResourceType, ResourceHandler> handlerMap = new HashMap<>();

    /**
     * Spring 自动注入所有 ResourceHandler 实现
     */
    @Autowired(required = false)
    private List<ResourceHandler> handlers;

    /**
     * 初始化注册所有资源处理器
     */
    @PostConstruct
    public void init() {
        if (handlers == null || handlers.isEmpty()) {
            return;
        }

        for (ResourceHandler handler : handlers) {
            // TaskResourceHandler 支持多种任务类型，需要特殊处理
            if (handler instanceof TaskResourceHandler) {
                TaskResourceHandler taskHandler = (TaskResourceHandler) handler;
                if (taskHandler.supports(ResourceType.MIGRATE_TASK)) {
                    handlerMap.put(ResourceType.MIGRATE_TASK, handler);
                }
                if (taskHandler.supports(ResourceType.SYNC_TASK)) {
                    handlerMap.put(ResourceType.SYNC_TASK, handler);
                }
            } else {
                ResourceType type = handler.getResourceType();
                if (type != null && !handlerMap.containsKey(type)) {
                    handlerMap.put(type, handler);
                }
            }
        }
    }

    /**
     * 根据资源类型获取对应的处理器
     * 
     * @param type 资源类型
     * @return 资源处理器，如果未找到返回 null
     */
    public ResourceHandler getHandler(ResourceType type) {
        if (type == null) {
            return null;
        }
        ResourceHandler handler = handlerMap.get(type);
        if (handler == null) {
            log.warn("Resource handler not found for type: {}", type);
        }
        return handler;
    }

}
