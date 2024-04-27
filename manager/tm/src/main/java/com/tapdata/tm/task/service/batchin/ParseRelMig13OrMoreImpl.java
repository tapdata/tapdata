package com.tapdata.tm.task.service.batchin;


import com.tapdata.tm.task.service.batchin.constant.KeyWords;
import com.tapdata.tm.task.service.batchin.dto.RelMigBaseDto;
import com.tapdata.tm.task.service.batchin.dto.TablePathInfo;
import com.tapdata.tm.task.service.batchin.entity.ParseParam;

import java.util.Map;

public class ParseRelMig13OrMoreImpl extends ParseBaseVersionRelMigImpl {

    public ParseRelMig13OrMoreImpl(ParseParam<RelMigBaseDto> param) {
        super(param);
    }

    @Override
    public TablePathInfo getTablePathInfo(Map<String, Object> contentMapping) {
        String tableId = String.valueOf(getFromMap(contentMapping, KeyWords.TABLE));
        Map<String, Object> content = parseMap(getFromMap(super.project, KeyWords.CONTENT));
        Map<String, Object> tables = parseMap(getFromMap(content, KeyWords.TABLES));
        Map<String, Object> tablePath = parseMap(getFromMap(tables, tableId));
        Map<String, Object> tableInfo = parseMap(getFromMap(tablePath, KeyWords.PATH));
        return new TablePathInfo(String.valueOf(tableInfo.get(KeyWords.DATABASE)),
                String.valueOf(tableInfo.get(KeyWords.SCHEMA)),
                String.valueOf(tableInfo.get(KeyWords.TABLE)));
    }
}
