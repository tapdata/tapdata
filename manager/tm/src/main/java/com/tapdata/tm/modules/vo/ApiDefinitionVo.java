package com.tapdata.tm.modules.vo;

import lombok.Data;

import java.util.List;

@Data
public class ApiDefinitionVo {
    List apis;
    List<ConnectionVo> connections;
}
