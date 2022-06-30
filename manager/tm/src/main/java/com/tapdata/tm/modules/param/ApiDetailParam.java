package com.tapdata.tm.modules.param;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class ApiDetailParam {
    private String id;
    /**
      "type":    "visitTotalLine",       可选值：visitTotalLine (访问行数),
                                             timeConsuming（耗时），
                                           speed（传输速率）
                                          latency（响应时间）
     */
    private String type;
    private Integer guanluary;
    private List<String> clientId=new ArrayList<>();
    private Long start;
    private Long end;
    private String status;
}
