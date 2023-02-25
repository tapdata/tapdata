package com.tapdata.tm.dag.convert;

import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.Dag;
import org.bson.Document;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;


/**
 * @Author: Zed
 * @Date: 2021/11/6
 * @Description:
 */
@ReadingConverter
public class DagDeserializeConvert implements Converter<Document, DAG> {
    @Override
    public DAG convert(Document document) {
        String json = JsonUtil.toJsonUseJackson(document);
        Dag dag = JsonUtil.parseJsonUseJackson(json, Dag.class);
        return DAG.build(dag);
    }
}
