package com.tapdata.tm.utils;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.util.NoPrimaryKeyTableSelectType;
import io.tapdata.entity.schema.TapIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MetadataInstancesFilterUtil {

    private MetadataInstancesFilterUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static List<MetadataInstancesDto> filterBySyncSourcePartitionTableEnable(DatabaseNode sourceNode, List<MetadataInstancesDto> metaList) {
        List<MetadataInstancesDto> realMetadata = new ArrayList<>();
        if (Objects.nonNull(sourceNode.getSyncSourcePartitionTableEnable())) {
            realMetadata.addAll(metaList.stream().filter(meta -> {
                if (Objects.isNull(meta.getPartitionInfo())) return true;
                String name = meta.getName();
                String masterTableId = meta.getPartitionMasterTableId();
                if (Boolean.TRUE.equals(sourceNode.getSyncSourcePartitionTableEnable())) {
                    return String.valueOf(name).equals(masterTableId);
                } else {
                    return !String.valueOf(name).equals(masterTableId);
                }
            }).collect(Collectors.toList()));
        } else {
            realMetadata.addAll(metaList);
        }
        return realMetadata;
    }

    public static List<String> getFilteredOriginalNames(List<MetadataInstancesDto> metaList, DatabaseNode sourceNode){
        return filterBySyncSourcePartitionTableEnable(sourceNode, metaList).stream()
                .map(metadataInstancesDto -> getMetadataInstancesDtoOriginalName(sourceNode, metadataInstancesDto))
                .filter(originalName -> isValidOriginalName(originalName, sourceNode))
                .collect(Collectors.toList());

    }
    public static Long countFilteredOriginalNames(List<MetadataInstancesDto> metaInstances, DatabaseNode sourceNode) {
        return filterBySyncSourcePartitionTableEnable(sourceNode, metaInstances).stream()
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
                                    boolean hasPrimaryKey = metadataInstancesDto.getFields().stream().anyMatch(field -> Boolean.TRUE.equals(field.getPrimaryKey()));
                                    if (hasPrimaryKey) return false;
                                    if (null == metadataInstancesDto.getIndices()) return true;
                                    for (TableIndex index : metadataInstancesDto.getIndices()) {
                                        if (Boolean.TRUE.equals(index.isUnique())) return false;
                                    }
                                }
                                return true;
                            };
                        case NoKeys:
                            return (Function<MetadataInstancesDto, Boolean>) metadataInstancesDto -> {
                                if (null != metadataInstancesDto.getFields()) {
                                    boolean hasPrimaryKey = metadataInstancesDto.getFields().stream().anyMatch(field -> Boolean.TRUE.equals(field.getPrimaryKey()));
                                    if (hasPrimaryKey) return true;
                                    if (null == metadataInstancesDto.getIndices()) return true;
                                    for (TableIndex index : metadataInstancesDto.getIndices()) {
                                        if (Boolean.TRUE.equals(index.isUnique())) return true;
                                    }
                                }
                                return false;
                            };
                        case OnlyPrimaryKey:
                            return (Function<MetadataInstancesDto, Boolean>) metadataInstancesDto -> {
                                if (null != metadataInstancesDto.getFields()) {
                                    boolean hasPrimaryKey = metadataInstancesDto.getFields().stream().anyMatch(field -> Boolean.TRUE.equals(field.getPrimaryKey()));
                                    if (!hasPrimaryKey) return true;
                                    if (null == metadataInstancesDto.getIndices()) return true;
                                    for (TableIndex index : metadataInstancesDto.getIndices()) {
                                        if (Boolean.TRUE.equals(index.isUnique())) return true;
                                    }
                                }
                                return false;
                            };
                        case OnlyUniqueIndex:
                            return (Function<MetadataInstancesDto, Boolean>) metadataInstancesDto -> {
                                if (null != metadataInstancesDto.getFields()) {
                                    boolean hasPrimaryKey = metadataInstancesDto.getFields().stream().anyMatch(field -> Boolean.TRUE.equals(field.getPrimaryKey()));
                                    if (hasPrimaryKey) return true;
                                    if (null == metadataInstancesDto.getIndices()) return true;
                                    for (TableIndex index : metadataInstancesDto.getIndices()) {
                                        if (Boolean.TRUE.equals(index.isUnique())) return false;
                                    }
                                }
                                return true;
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
