package com.tapdata.tm.group.vo;

import com.tapdata.tm.modules.vo.ModulesListVo;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ModuleWithGroupVo extends ModulesListVo {
    private String groupId;
    private String groupName;
}
