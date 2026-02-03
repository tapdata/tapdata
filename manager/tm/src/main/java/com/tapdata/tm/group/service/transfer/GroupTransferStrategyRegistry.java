package com.tapdata.tm.group.service.transfer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * 传输策略注册中心
 * 负责传输策略的注册和获取
 * 通过 Spring 自动注入所有 GroupTransferStrategy 实现
 */
@Slf4j
@Component
public class GroupTransferStrategyRegistry {

    /**
     * 传输类型到策略的映射
     */
    private final Map<GroupTransferType, GroupTransferStrategy> strategyMap = new EnumMap<>(GroupTransferType.class);

    /**
     * Spring 自动注入所有 GroupTransferStrategy 实现
     */
    @Autowired(required = false)
    private List<GroupTransferStrategy> strategies;

    /**
     * 初始化注册所有传输策略
     */
    @PostConstruct
    public void init() {
        if (strategies == null || strategies.isEmpty()) {
            log.warn("No GroupTransferStrategy implementations found");
            return;
        }

        for (GroupTransferStrategy strategy : strategies) {
            GroupTransferType type = strategy.getType();
            if (type != null && !strategyMap.containsKey(type)) {
                strategyMap.put(type, strategy);
                log.info("Registered GroupTransferStrategy: {} -> {}", type, strategy.getClass().getSimpleName());
            }
        }
    }

    /**
     * 根据传输类型获取对应的策略
     *
     * @param type 传输类型
     * @return 传输策略，如果未找到返回 null
     */
    public GroupTransferStrategy getStrategy(GroupTransferType type) {
        if (type == null) {
            return null;
        }
        GroupTransferStrategy strategy = strategyMap.get(type);
        if (strategy == null) {
            log.warn("GroupTransferStrategy not found for type: {}", type);
        }
        return strategy;
    }

    /**
     * 获取默认策略（FILE）
     *
     * @return 默认传输策略
     */
    public GroupTransferStrategy getDefaultStrategy() {
        return getStrategy(GroupTransferType.FILE);
    }

    /**
     * 获取所有已注册的传输策略
     *
     * @return 传输策略列表
     */
    public List<GroupTransferStrategy> getAllStrategies() {
        return List.copyOf(strategyMap.values());
    }
}

