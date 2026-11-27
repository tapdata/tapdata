package com.tapdata.tm.throttle.service;

import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.throttle.dto.TaskThrottleSampleConfigView;
import com.tapdata.tm.throttle.entity.AgentThrottleCapabilityEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages global system setting {@code cdcSampleRefreshIntervalSeconds} (5-300s, default 30s),
 * increments sampleConfigVersion on valid updates, and summarizes rollout statuses per Agent.
 */
@Service
@Slf4j
public class TaskThrottleSampleConfigService {

    private final MongoOperations mongo;
    private final Clock clock;
    private final AtomicLong configVersion = new AtomicLong(1L);
    private volatile int cachedIntervalSeconds = ThrottleConstants.DEFAULT_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS;
    private volatile long lastUpdatedAt;

    @Autowired
    public TaskThrottleSampleConfigService(MongoOperations mongo) {
        this(mongo, Clock.systemUTC());
    }

    public TaskThrottleSampleConfigService(MongoOperations mongo, Clock clock) {
        this.mongo = mongo;
        this.clock = clock == null ? Clock.systemUTC() : clock;
        this.lastUpdatedAt = this.clock.millis();
    }

    /**
     * Validates an integer refresh interval within [5, 300] seconds. Rejects null, decimals, non-numbers, or out-of-range inputs.
     */
    public int validateIntervalValue(Object rawValue) {
        if (rawValue == null) {
            throw new BizException(ThrottleConstants.ERR_INVALID_RATE, "cdcSampleRefreshIntervalSeconds must not be null");
        }
        int parsed;
        if (rawValue instanceof Integer i) {
            parsed = i;
        } else if (rawValue instanceof Long l) {
            if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
                throw new BizException(ThrottleConstants.ERR_INVALID_RATE, "cdcSampleRefreshIntervalSeconds out of range");
            }
            parsed = l.intValue();
        } else if (rawValue instanceof Number n) {
            double d = n.doubleValue();
            if (!Double.isFinite(d) || d != Math.rint(d)) {
                throw new BizException(ThrottleConstants.ERR_INVALID_RATE, "cdcSampleRefreshIntervalSeconds must be an integer");
            }
            parsed = (int) d;
        } else {
            String str = String.valueOf(rawValue).trim();
            if (str.isEmpty() || str.contains(".")) {
                throw new BizException(ThrottleConstants.ERR_INVALID_RATE, "cdcSampleRefreshIntervalSeconds must be an integer");
            }
            try {
                parsed = Integer.parseInt(str);
            } catch (NumberFormatException ex) {
                throw new BizException(ThrottleConstants.ERR_INVALID_RATE, "cdcSampleRefreshIntervalSeconds must be an integer");
            }
        }
        if (parsed < ThrottleConstants.MIN_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS
                || parsed > ThrottleConstants.MAX_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS) {
            throw new BizException(
                    ThrottleConstants.ERR_INVALID_RATE,
                    "cdcSampleRefreshIntervalSeconds must be between "
                            + ThrottleConstants.MIN_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS
                            + " and " + ThrottleConstants.MAX_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS);
        }
        return parsed;
    }

    public ConfigSnapshot getCurrentConfig() {
        Settings settings = mongo == null ? null : mongo.findOne(
                Query.query(Criteria.where("key").is(ThrottleConstants.SETTING_KEY_CDC_SAMPLE_REFRESH_INTERVAL)),
                Settings.class);
        if (settings != null) {
            Object raw = settings.getValue() != null ? settings.getValue() : settings.getDefault_value();
            if (raw != null && !String.valueOf(raw).isBlank()) {
                try {
                    cachedIntervalSeconds = validateIntervalValue(raw);
                } catch (Exception ignored) {
                    // Fallback cleanly to last valid or default 30s
                }
            }
            if (settings.getThrottle_config_version() != null && settings.getThrottle_config_version() > 0L) {
                configVersion.accumulateAndGet(settings.getThrottle_config_version(), Math::max);
            }
            if (settings.getLast_update() != null && settings.getLast_update() > 0) {
                lastUpdatedAt = settings.getLast_update().longValue() * 1000L;
            }
        }
        return new ConfigSnapshot(cachedIntervalSeconds, configVersion.get(), lastUpdatedAt);
    }

    public TaskThrottleSampleConfigView updateInterval(Object rawInterval) {
        int validSeconds = validateIntervalValue(rawInterval);
        long now = clock.millis();
        long persistedBase = readPersistedVersion();
        long base = Math.max(configVersion.get(), Math.max(1L, persistedBase));
        long nextVersion = base + 1L;
        configVersion.accumulateAndGet(nextVersion, Math::max);
        long effectiveVersion = configVersion.get();

        if (mongo != null) {
            Query query = Query.query(Criteria.where("key").is(ThrottleConstants.SETTING_KEY_CDC_SAMPLE_REFRESH_INTERVAL));
            Update update = new Update()
                    .set("key", ThrottleConstants.SETTING_KEY_CDC_SAMPLE_REFRESH_INTERVAL)
                    .set("category", "Job")
                    .set("category_sort", 20)
                    .set("sort", 30)
                    .set("scope", "global")
                    .set("key_label", "Incremental data size sampling interval")
                    .set("documentation", "Sampling refresh interval (seconds) for CDC table x DML sizing")
                    .set("default_value", String.valueOf(ThrottleConstants.DEFAULT_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS))
                    .set("value", String.valueOf(validSeconds))
                    .set("hot_reloading", true)
                    .set("user_visible", true)
                    .set("min", ThrottleConstants.MIN_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS)
                    .set("max", ThrottleConstants.MAX_CDC_SAMPLE_REFRESH_INTERVAL_SECONDS)
                    .set("throttle_config_version", effectiveVersion)
                    .set("last_update", (int) (now / 1000L));
            mongo.upsert(query, update, Settings.class);
        }

        this.cachedIntervalSeconds = validSeconds;
        this.lastUpdatedAt = now;
        return getConfigView();
    }

    /**
     * Hook invoked when legacy Settings endpoints update {@code cdcSampleRefreshIntervalSeconds}.
     */
    public void validateAndAdvanceVersion(Object rawValue) {
        int valid = validateIntervalValue(rawValue);
        long now = clock.millis();
        long persistedBase = readPersistedVersion();
        long nextVersion = Math.max(configVersion.get(), Math.max(1L, persistedBase)) + 1L;
        configVersion.accumulateAndGet(nextVersion, Math::max);
        long effectiveVersion = configVersion.get();
        if (mongo != null) {
            Query query = Query.query(Criteria.where("key").is(ThrottleConstants.SETTING_KEY_CDC_SAMPLE_REFRESH_INTERVAL));
            mongo.upsert(query, new Update()
                    .set("key", ThrottleConstants.SETTING_KEY_CDC_SAMPLE_REFRESH_INTERVAL)
                    .set("value", String.valueOf(valid))
                    .set("throttle_config_version", effectiveVersion)
                    .set("last_update", (int) (now / 1000L)), Settings.class);
        }
        cachedIntervalSeconds = valid;
        lastUpdatedAt = now;
    }

    private long readPersistedVersion() {
        if (mongo == null) {
            return 0L;
        }
        Settings settings = mongo.findOne(
                Query.query(Criteria.where("key").is(ThrottleConstants.SETTING_KEY_CDC_SAMPLE_REFRESH_INTERVAL)),
                Settings.class);
        if (settings != null && settings.getThrottle_config_version() != null && settings.getThrottle_config_version() > 0L) {
            return settings.getThrottle_config_version();
        }
        return 0L;
    }

    public TaskThrottleSampleConfigView getConfigView() {
        ConfigSnapshot snap = getCurrentConfig();
        List<TaskThrottleSampleConfigView.AgentSampleStatus> agentStatuses = new ArrayList<>();
        if (mongo != null) {
            List<AgentThrottleCapabilityEntity> caps = mongo.findAll(AgentThrottleCapabilityEntity.class);
            if (caps != null) {
                for (AgentThrottleCapabilityEntity cap : caps) {
                    if (cap == null) {
                        continue;
                    }
                    String status;
                    String reason = null;
                    if (!cap.isThrottleSupported() || !ThrottleConstants.METER_VERSION.equals(cap.getMeterVersion())) {
                        status = TaskThrottleSampleConfigView.STATUS_UNSUPPORTED_LEGACY;
                        reason = "Agent does not report compatible meterVersion " + ThrottleConstants.METER_VERSION;
                    } else if (cap.getCdcSampleConfigVersion() >= snap.configVersion()
                            && cap.getCdcSampleRefreshIntervalSeconds() == snap.intervalSeconds()) {
                        status = TaskThrottleSampleConfigView.STATUS_APPLIED;
                    } else {
                        status = TaskThrottleSampleConfigView.STATUS_APPLYING;
                        reason = "Pending delta rollout (current=" + cap.getCdcSampleRefreshIntervalSeconds()
                                + "s, v" + cap.getCdcSampleConfigVersion() + ")";
                    }
                    agentStatuses.add(TaskThrottleSampleConfigView.AgentSampleStatus.builder()
                            .agentId(cap.getAgentId())
                            .appliedIntervalSeconds(cap.getCdcSampleRefreshIntervalSeconds())
                            .appliedConfigVersion(cap.getCdcSampleConfigVersion())
                            .status(status)
                            .reason(reason)
                            .build());
                }
            }
        }
        return TaskThrottleSampleConfigView.builder()
                .key(ThrottleConstants.SETTING_KEY_CDC_SAMPLE_REFRESH_INTERVAL)
                .intervalSeconds(snap.intervalSeconds())
                .configVersion(snap.configVersion())
                .updatedAt(snap.updatedAt())
                .agentStatuses(agentStatuses)
                .build();
    }

    public record ConfigSnapshot(int intervalSeconds, long configVersion, long updatedAt) {
    }
}
