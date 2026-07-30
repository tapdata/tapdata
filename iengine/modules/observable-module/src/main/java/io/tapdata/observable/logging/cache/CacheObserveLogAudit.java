package io.tapdata.observable.logging.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

class CacheObserveLogAudit {
    private static final String EVICT_MESSAGE =
            "CACHE_OBSERVE_LOG_EVICT taskId={} taskName={} generation={} file={} "
                    + "dataLoss={} taskCacheBytes={} totalCacheBytes={}";

    private final Logger logger;

    CacheObserveLogAudit() {
        this(LoggerFactory.getLogger(CacheObserveLogAudit.class));
    }

    CacheObserveLogAudit(Logger logger) {
        this.logger = logger;
    }

    void evict(String taskId,
               String taskName,
               long generation,
               boolean dataLoss,
               String file,
               long taskCacheBytes,
               long totalCacheBytes) {
        if (dataLoss) {
            logger.warn(
                    EVICT_MESSAGE,
                    taskId,
                    taskName,
                    generation,
                    file,
                    true,
                    taskCacheBytes,
                    totalCacheBytes);
            return;
        }
        logger.info(
                EVICT_MESSAGE,
                taskId,
                taskName,
                generation,
                file,
                false,
                taskCacheBytes,
                totalCacheBytes);
    }

    void deleteFailed(String taskId, String taskName, Path file, Exception exception) {
        logger.warn(
                "CACHE_OBSERVE_LOG_DELETE_FAILED taskId={} taskName={} file={} error={}",
                taskId,
                taskName,
                file,
                exception.getMessage(),
                exception);
    }
}
