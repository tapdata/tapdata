package io.tapdata.observable.logging.debug;

import cn.hutool.core.lang.ClassScanner;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.cache.ObjectSerializer;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CachePersistenceException;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceUnit;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.EhcacheManager;
import org.ehcache.spi.serialization.SerializerException;
import org.springframework.context.annotation.ClassPathBeanDefinitionScanner;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/11/19 14:55
 */
public final class DataCacheFactory {

    @Getter
    private static final DataCacheFactory instance = new DataCacheFactory();

    public static SerializeConfig dataSerializeConfig = new SerializeConfig();
    private static Map<String, com.alibaba.fastjson.serializer.ObjectSerializer> dataSerializeConfigMap = new HashMap<>();
    static {
        dataSerializeConfigMap.put(DateTime.class.getName(), (serializer, object, fieldName, fieldType, features) -> {
            serializer.write(((DateTime)object).toDate());
        });
        dataSerializeConfigMap.put(ObjectId.class.getName(), (serializer, object, fieldName, fieldType, features) -> {
            serializer.write(((ObjectId)object).toHexString());
        });
        dataSerializeConfigMap.put(Timestamp.class.getName(), (serializer, object, fieldName, fieldType, features) -> {
            serializer.write(new Date(((Timestamp)object).getTime()));
        });
        dataSerializeConfig.put(DateTime.class, dataSerializeConfigMap.get(DateTime.class.getName()));
        dataSerializeConfig.put(ObjectId.class, dataSerializeConfigMap.get(ObjectId.class.getName()));
        dataSerializeConfig.put(Timestamp.class, dataSerializeConfigMap.get(Timestamp.class.getName()));
        com.alibaba.fastjson.serializer.ObjectSerializer objectSerializer = (serializer, object, fieldName, fieldType, features) -> {
            if (object != null) {
                TapValue<?, ?> tapValue = ((TapValue<?, ?>) object);
                if (tapValue.getValue() instanceof String) {
                    serializer.write(tapValue.getValue());
                    return;
                }
                Object originVal = tapValue.getOriginValue();
                if (originVal == null) {
                    originVal = ((TapValue<?, ?>) object).getValue();
                }
                com.alibaba.fastjson.serializer.ObjectSerializer writer = null;
                if (originVal != null) {
                    writer = dataSerializeConfigMap.get(originVal.getClass().getName());
                }
                if (originVal == null) {
                    serializer.writeNull();
                    return;
                }
                if (writer == null)
                    writer = dataSerializeConfig.getObjectWriter(originVal.getClass());
                if (writer != null)
                    writer.write(serializer, originVal, fieldName, fieldType, features);
                else
                    serializer.write(originVal);
                return;
            }
            serializer.write(object);
        };
        ClassScanner.scanPackageBySuper("io.tapdata.entity.schema.value", TapValue.class).forEach(clazz -> {
            dataSerializeConfig.put(clazz, objectSerializer);
        });
    }

    private Map<String, DataCache> taskDataCache;
    private EhcacheManager cacheManager;

    private Integer maxHeapEntries = 10;
    private Integer maxOffHeapSize = 10; // Mb
    private Integer maxDiskSize = 100;    // Mb
    private Logger logger = LogManager.getLogger(DataCacheFactory.class);

    private DataCacheFactory() {
        taskDataCache = new HashMap<>();
        initCacheManager();
    }

    private void initCacheManager() {
        File file = new File(getDataCacheDirectory());
        if (!file.exists()) {
            CommonUtils.ignoreAnyError(() -> file.mkdirs(), "cache data");
        }
        cacheManager = (EhcacheManager) CacheManagerBuilder.newCacheManagerBuilder()
                .with(CacheManagerBuilder.persistence(file))
                .withSerializer(Object.class, ObjectSerializer.class)
                .build(true);
    }

    public String getDataCacheDirectory() {
        ConfigurationCenter configurationCenter = BeanUtil.getBean(ConfigurationCenter.class);
        String workerDir = null;
        if (configurationCenter != null && configurationCenter.getConfig(ConfigurationCenter.WORK_DIR) != null) {
            workerDir = configurationCenter.getConfig(ConfigurationCenter.WORK_DIR).toString();
        }
        if (StringUtils.isBlank(workerDir)) {
            workerDir = System.getenv("TAPDATA_WORK_DIR");
        }
        if (StringUtils.isBlank(workerDir)) {
            workerDir = ".";
        }
        workerDir = workerDir + File.separator + "cache";
        return workerDir;
    }

    /**
     * create data cache by task id
     * @param taskId task id
     * @return data cache
     */
    public DataCache getDataCache(String taskId) {
        return taskDataCache.computeIfAbsent(taskId, k -> buildDataCache(taskId));
    }

    public void removeDataCache(String taskId) {
        DataCache dataCache = taskDataCache.remove(taskId);
        if (dataCache != null) {
            try {
                dataCache.destroy();
                cacheManager.destroyCache(taskId);
            } catch (CachePersistenceException e) {
                logger.error("Destroy cache for {} failed", taskId, e);
            }
        }
    }

    private DataCache buildDataCache(String taskId) {

        if (maxHeapEntries == null) maxHeapEntries = 10;
        if (maxOffHeapSize == null) maxOffHeapSize = 10;
        if (maxDiskSize == null) maxDiskSize = 50;

        try {
            ResourcePoolsBuilder builder = ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .heap(maxHeapEntries, EntryUnit.ENTRIES)
                    .offheap(maxOffHeapSize, MemoryUnit.MB)
                    .disk(maxDiskSize, MemoryUnit.MB, true);

            Cache<String, DataCache.CacheItem> cache = cacheManager.createCache(taskId, CacheConfigurationBuilder
                    .newCacheConfigurationBuilder(String.class, DataCache.CacheItem.class, builder)
                    .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMinutes(10)))
            );

            return new DataCache(taskId, null, cache);
        } catch (Exception e) {
            logger.error("Create data cache failed {}", e.getMessage(), e);
            return new DataCache(taskId, null, null);
        }
    }
}
