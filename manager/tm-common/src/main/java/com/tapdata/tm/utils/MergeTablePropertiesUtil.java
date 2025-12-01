package com.tapdata.tm.utils;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MergeTablePropertiesUtil {
    public static void initLookupMergeProperties(List<MergeTableProperties> mergeProperties, Map<String, List<MergeTableProperties>> lookupMap) {
        for (MergeTableProperties mergeProperty : mergeProperties) {
            recursiveGetLookupList(mergeProperty,lookupMap);
        }
    }

    public static void recursiveGetLookupList(MergeTableProperties mergeTableProperties,Map<String, List<MergeTableProperties>> lookupMap) {
        List<MergeTableProperties> lookupList = new ArrayList<>();
        List<MergeTableProperties> children = mergeTableProperties.getChildren();
        if (CollectionUtils.isEmpty(children)) return;
        for (MergeTableProperties child : children) {
            lookupList.add(child);
            recursiveGetLookupList(child,lookupMap);
        }
        lookupMap.put(mergeTableProperties.getId(), lookupList);
    }

    /**
     * 递归查找MergeTableProperties，包括children
     * @param mergeProperties 属性列表
     * @param id 要查找的ID
     * @return 找到的MergeTableProperties，未找到返回null
     */
    public static MergeTableProperties findMergeTablePropertiesById(List<MergeTableProperties> mergeProperties, String id) {
        if(CollectionUtils.isEmpty(mergeProperties) || StringUtils.isBlank(id)){
            return null;
        }
        for(MergeTableProperties property : mergeProperties){
            if(id.equals(property.getId())){
                return property;
            }
            // 递归查找children
            List<MergeTableProperties> children = property.getChildren();
            if(CollectionUtils.isNotEmpty(children)){
                MergeTableProperties found = findMergeTablePropertiesById(children, id);
                if(null != found){
                    return found;
                }
            }
        }
        return null;
    }

    /**
     * 递归查找多个MergeTableProperties，包括children
     * @param mergeProperties 属性列表
     * @param ids 要查找的ID列表
     * @return 找到的MergeTableProperties列表
     */
    public static List<MergeTableProperties> findMergeTablePropertiesByIds(List<MergeTableProperties> mergeProperties, List<String> ids) {
        List<MergeTableProperties> result = new ArrayList<>();
        if(CollectionUtils.isEmpty(mergeProperties) || CollectionUtils.isEmpty(ids)){
            return result;
        }
        for(String id : ids){
            MergeTableProperties found = findMergeTablePropertiesById(mergeProperties, id);
            if(null != found){
                result.add(found);
            }
        }
        return result;
    }

    public static Map<String,List<String>> getMergeTablePropertiesIdList(DAG dag) {
        Map<String,List<String> > cacheIdMap = new HashMap<>();
        dag.getNodes().stream().filter(node -> node instanceof MergeTableNode).forEach(node -> {
            MergeTableNode mergeTableNode = (MergeTableNode) node;
            List<String> cacheIdList = new ArrayList<>();
            Map<String, List<MergeTableProperties>> lookupMap = new HashMap<>();
            initLookupMergeProperties(mergeTableNode.getMergeProperties(),lookupMap);
            for (List<MergeTableProperties> lookupList : lookupMap.values()) {
                cacheIdList.addAll(lookupList.stream().map(MergeTableProperties::getId).toList());
            }
            cacheIdMap.put(node.getId(), cacheIdList);
        });
        return cacheIdMap;
    }

    public static Map<String,List<Map<String, String>>> getMergeTablePropertiesInfoList(DAG dag) {
        Map<String,List<Map<String, String>>> cacheIdMap = new HashMap<>();
        dag.getNodes().stream().filter(node -> node instanceof MergeTableNode).forEach(node -> {
            MergeTableNode mergeTableNode = (MergeTableNode) node;
            List<Map<String, String>> cacheIdList = new ArrayList<>();
            Map<String, List<MergeTableProperties>> lookupMap = new HashMap<>();
            initLookupMergeProperties(mergeTableNode.getMergeProperties(),lookupMap);
            for (List<MergeTableProperties> lookupList : lookupMap.values()) {
                cacheIdList.addAll(lookupList.stream().map(mergeTableProperties -> {
                    Map<String, String> map = new HashMap<>();
                    map.put("id", mergeTableProperties.getId());
                    map.put("tableName", mergeTableProperties.getTableName());
                    return map;
                }).toList());
            }
            cacheIdMap.put(node.getId(), cacheIdList);
        });
        return cacheIdMap;
    }


}
