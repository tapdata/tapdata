package com.tapdata.tm.monitor.param;

import lombok.Data;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.Serializable;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/22 18:59 Create
 */
@Data
public class IdFilterPageParam extends IdParam {
    private String filter;
    private Long skip;
    private Integer limit;

    public void setSkip(Long skip) {
        this.skip = null == skip ? 0 : skip;
    }

    public void setLimit(Integer limit) {
        this.limit = null == limit ? 20 : limit;
    }
}
