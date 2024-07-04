package com.tapdata.tm.utils;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.util.NoPrimaryKeyTableSelectType;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MetadataInstancesFilterUtil {

    private MetadataInstancesFilterUtil() {
        throw new IllegalStateException("Utility class");
    }
    public static List<String> getFilteredOriginalNames(List<MetadataInstancesDto> metaList, DatabaseNode sourceNode){
        return metaList.stream()
                .map(metadataInstancesDto -> getMetadataInstancesDtoOriginalName(sourceNode, metadataInstancesDto))
                .filter(originalName -> isValidOriginalName(originalName, sourceNode))
                .collect(Collectors.toList());

    }
    public static Long countFilteredOriginalNames(List<MetadataInstancesDto> metaInstances, DatabaseNode sourceNode) {
        return metaInstances.stream()
                .map(metadataInstancesDto -> getMetadataInstancesDtoOriginalName(sourceNode, metadataInstancesDto))
                .filter(originalName -> isValidOriginalName(originalName, sourceNode))
                .count();
    }
    private static String getMetadataInstancesDtoOriginalName(DatabaseNode sourceNode,MetadataInstancesDto metadataInstances){
        Function<MetadataInstancesDto, Boolean> filterTableByNoPrimaryKey = Optional
                .of(NoPrimaryKeyTableSelectType.parse(sourceNode.getNoPrimaryKeyTableSelectType()))
                .map(type -> {
                    switch (type) {
                        case HasKeys:
                            return (Function<MetadataInstancesDto, Boolean>) metadataInstancesDto -> {
                                if (null != metadataInstancesDto.getFields()) {
                                    for (Field field : metadataInstancesDto.getFields()) {
                                        if (Boolean.TRUE.equals(field.getPrimaryKey())) return false;
                                    }
                                }
                                return true;
                            };
                        case NoKeys:
                            return (Function<MetadataInstancesDto, Boolean>) metadataInstancesDto -> {
                                if (null != metadataInstancesDto.getFields()) {
                                    for (Field field : metadataInstancesDto.getFields()) {
                                        if (Boolean.TRUE.equals(field.getPrimaryKey())) return true;
                                    }
                                }
                                return false;
                            };
                        default:
                    }
                    return null;
                }).orElse(metadataInstancesDto -> false);
        if (filterTableByNoPrimaryKey.apply(metadataInstances)) {
            return null;
        }
        return metadataInstances.getOriginalName();
    }

    private static boolean isValidOriginalName(String originalName, DatabaseNode sourceNode) {
        if (null == originalName) {
            return false;
        } else if (StringUtils.isEmpty(sourceNode.getTableExpression())) {
            return false;
        } else {
            return Pattern.matches(sourceNode.getTableExpression(), originalName);
        }
    }
}
