package com.tapdata.tm.group.strategy;

import com.tapdata.tm.commons.task.dto.ImportModeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 导入策略注册中心
 * 管理所有导入策略
 */
@Slf4j
@Component
public class ImportStrategyRegistry {

    private final Map<ImportModeEnum, ImportStrategy> strategyMap = new HashMap<>();

    @Autowired(required = false)
    private List<ImportStrategy> strategies;

    @PostConstruct
    public void init() {
        if (strategies == null || strategies.isEmpty()) {
            return;
        }
        for (ImportStrategy strategy : strategies) {
            ImportModeEnum mode = strategy.getImportMode();
            if (mode != null && !strategyMap.containsKey(mode)) {
                strategyMap.put(mode, strategy);
            }
        }
    }

    /**
     * 根据导入模式获取对应的策略
     * 
     * @param mode 导入模式
     * @return 导入策略，如果未找到返回默认策略（GROUP_IMPORT）
     */
    public ImportStrategy getStrategy(ImportModeEnum mode) {
        if (mode == null) {
            mode = ImportModeEnum.GROUP_IMPORT;
        }

        ImportStrategy strategy = strategyMap.get(mode);
        if (strategy == null) {
            strategy = strategyMap.get(ImportModeEnum.GROUP_IMPORT);
        }
        return strategy;
    }

    /**
     * 获取所有已注册的导入策略
     * 
     * @return 导入策略列表
     */
    public List<ImportStrategy> getAllStrategies() {
        return List.copyOf(strategyMap.values());
    }
}
