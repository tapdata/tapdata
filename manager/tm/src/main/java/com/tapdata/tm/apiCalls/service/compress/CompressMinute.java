package com.tapdata.tm.apiCalls.service.compress;

import com.tapdata.tm.apiCalls.entity.WorkerCallEntity;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/3 11:23 Create
 * @description
 */
@Service
public class CompressMinute implements Compress, InitializingBean {
    public static final long STEP = 60 * 1000L;

    @Override
    public void afterPropertiesSet() {
        Compress.Factory.register(Compress.Type.MINUTE, this);
    }

    @Override
    public Long compressTime(WorkerCallEntity e) {
        return (e.getTimeStart() / STEP) * STEP;
    }

    @Override
    public long plus(long time) {
        return time + STEP;
    }
}
