package io.tapdata.aspect;

import io.tapdata.entity.Usage;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/10/9 16:04 Create
 * @description
 */
public class CpuMemUsageAspect extends DataFunctionAspect<CpuMemUsageAspect> {
    Usage usage;
    public CpuMemUsageAspect(Usage usage) {
        this.usage = usage;
    }

    public Usage getUsage() {
        return usage;
    }
}
