package com.tapdata.tm.v2.api.monitor.main.param;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/31 09:33 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class TopWorkerInServerParam extends QueryBase {
    String serverId;

    /**
     * CPU: 仅查询cpu分布
     * ALL: 查询cpu分布 + Worker列表
     * */
    String tag;

    @Getter
    public enum TAG {
        CPU("CPU"),
        ALL("ALL")
        ;

        final String value;

        TAG(String t) {
            this.value = t;
        }

        public static TAG fromValue(String v) {
            for (TAG tag : values()) {
                if (tag.value.equals(v)) {
                    return tag;
                }
            }
            return ALL;
        }
    }
}
