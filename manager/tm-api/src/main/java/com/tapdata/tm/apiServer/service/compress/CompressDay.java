package com.tapdata.tm.apiServer.service.compress;

import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.base.exception.BizException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/3 11:25 Create
 * @description
 */
@Service
public class CompressDay implements Compress, InitializingBean {
    public static final long STEP = 24 * 60 * 60 * 1000L;

    @Override
    public void checkTimeRange(long from, long end) {
        int days = ((Number) ((end - from) / STEP)).intValue();
        if (days > 180) {
            throw new BizException("api.call.chart.day.time.range.too.large", 180);
        }
    }

    @Override
    public void afterPropertiesSet() {
        Compress.Factory.register(Type.DAY, this);
    }

    @Override
    public Long compressTime(WorkerCallEntity e) {
        return fixTme(e.getTimeStart());
    }

    @Override
    public long fixTme(long time) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault());
        return localDateTime.toLocalDate().atStartOfDay().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    @Override
    public long defaultFrom(long end) {
        return end - 7 * STEP;
    }

    @Override
    public long plus(long time) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault());
        return localDateTime.toLocalDate().atStartOfDay().plusDays(1).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
}
