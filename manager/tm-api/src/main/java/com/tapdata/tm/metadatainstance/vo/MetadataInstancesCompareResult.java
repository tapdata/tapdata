package com.tapdata.tm.metadatainstance.vo;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.schema.DifferenceTypeEnum;
import com.tapdata.tm.commons.schema.MetadataInstancesCompareDto;
import lombok.Data;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Data
public class MetadataInstancesCompareResult {
    private Page<MetadataInstancesCompareDto> compareDtos;
    private List<MetadataInstancesCompareDto> invalidApplyDtos;
    private String status;
    private Date finishTime;
    private Date targetSchemaLoadTime;
    private Map<String,Integer> differentFieldNumberMap = new HashMap<>();

    public MetadataInstancesCompareResult() {
       for (DifferenceTypeEnum differenceTypeEnum : DifferenceTypeEnum.values()) {
           differentFieldNumberMap.put(differenceTypeEnum.name(), 0);
           differentFieldNumberMap.put(differenceTypeEnum.name() + "Apply", 0);
       }
    }

    public void computeDifferentFieldNumber(DifferenceTypeEnum type) {
        differentFieldNumberMap.merge(type.name(), 1, Integer::sum);
    }

    public void computeApplyDifferentFieldNumber(DifferenceTypeEnum type) {
        differentFieldNumberMap.merge(type.name() + "Apply", 1, Integer::sum);
    }

}
