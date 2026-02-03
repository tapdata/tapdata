package com.tapdata.tm.v2.api.monitor.main.dto;

import lombok.Data;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/31 09:45 Create
 * @description
 */
@Data
public class ValueBase {
    long queryFrom;
    long queryEnd;
    int granularity;

    @Data
    public static class Item {
        protected long ts;

        boolean empty;

        public <T extends ValueBase.Item>T empty() {
            this.empty = true;
            return (T) this;
        }
    }
}
