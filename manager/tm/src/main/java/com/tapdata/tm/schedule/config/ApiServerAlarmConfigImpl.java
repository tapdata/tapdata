package com.tapdata.tm.schedule.config;

import com.tapdata.tm.alarm.service.ApiServerAlarmConfig;
import com.tapdata.tm.alarmrule.service.AlarmRuleService;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import lombok.Setter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/12 10:02 Create
 * @description
 */
@Service
@Setter(onMethod_ = {@Autowired})
public class ApiServerAlarmConfigImpl implements ApiServerAlarmConfig, InitializingBean {
    static final Map<String, Map<AlarmKeyEnum, AlarmRuleDto>> CONFIG = new ConcurrentHashMap<>();

    private AlarmRuleService alarmRuleService;

    @Override
    public void afterPropertiesSet() throws Exception {
        updateConfig();
    }

    @Override
    public void updateConfig() {
        synchronized (CONFIG) {
            clean();
            List<String> keys = getKeys();
            if (keys.isEmpty()) {
                return;
            }
            Criteria criteria = Criteria.where("key").in(keys);
            Query query = Query.query(criteria);
            List<AlarmRuleDto> all = alarmRuleService.findAll(query);
            if (all.isEmpty()) {
                return;
            }
            all.stream().filter(Objects::nonNull).forEach(rule -> CONFIG.computeIfAbsent(SYSTEM, k -> new ConcurrentHashMap<>()).put(rule.getKey(), rule));
        }
    }

    @Override
    public void clean() {
        if (!CONFIG.isEmpty()) {
            CONFIG.clear();
        }
    }

    @Override
    public void remove(String apiId, AlarmKeyEnum alarmKeyEnum) {
        synchronized (CONFIG) {
            Optional.ofNullable(apiId)
                    .map(CONFIG::get)
                    .map(e -> {
                        e.remove(alarmKeyEnum);
                        return CONFIG.get(SYSTEM);
                    }).ifPresent(map -> map.remove(alarmKeyEnum));
        }
    }

    @Override
    public AlarmRuleDto config(String apiId, AlarmKeyEnum alarmKeyEnum) {
        synchronized (CONFIG) {
            return Optional.ofNullable(alarmKeyEnum)
                    .map(e -> Optional.ofNullable(apiId)
                            .map(CONFIG::get)
                            .orElse(CONFIG.get(SYSTEM)))
                    .map(map -> map.get(alarmKeyEnum))
                    .orElse(null);
        }
    }

    List<String> getKeys() {
        List<String> keys = new ArrayList<>();
        for (AlarmKeyEnum value : AlarmKeyEnum.values()) {
            if (value.getType().equals(AlarmKeyEnum.Constant.TYPE_API_SERVER)) {
                keys.add(value.name());
            }
        }
        return keys;
    }
}
