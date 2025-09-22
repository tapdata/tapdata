package com.tapdata.tm.apiServer.service.compress;

import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.base.exception.BizException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/3 11:24 Create
 * @description
 */
@Service
public class CompressHours implements Compress, InitializingBean {
    public static final long STEP = 60 * 60 * 1000L;

    @Override
    public void checkTimeRange(long from, long end) {
        int days = ((Number) ((end - from) / STEP)).intValue();
        if (days > 240) {
            throw new BizException("api.call.chart.hours.time.range.too.large", 240);
        }
    }

    @Override
    public void afterPropertiesSet() {
        Compress.Factory.register(Type.HOUR, this);
    }

    @Override
    public Long compressTime(WorkerCallEntity e) {
        return fixTme(e.getTimeStart());
    }

    @Override
    public long defaultFrom(long end) {
        return end - 5 * STEP;
    }

    @Override
    public long fixTme(long time) {
        return (time / STEP) * STEP;
    }

    @Override
    public long plus(long time) {
        return time + STEP;
    }
}
