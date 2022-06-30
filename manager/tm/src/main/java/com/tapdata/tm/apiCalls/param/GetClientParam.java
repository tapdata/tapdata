package com.tapdata.tm.apiCalls.param;

import lombok.Data;

import java.util.List;

@Data
public class GetClientParam {
    private List<String> moduleIdList;
}
