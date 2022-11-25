package com.tapdata.tm.dag.convert;

import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.Dag;
import org.bson.Document;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;


/**
 * @Author: Zed
 * @Date: 2021/11/6
 * @Description:
 */
@WritingConverter
public class DagSerializeConvert implements Converter<DAG, Document> {
    @Override
    public Document convert(DAG dag) {
        Dag dag1 = dag.toDag();
        String json = JsonUtil.toJsonUseJackson(dag1);
        return Document.parse(json);
    }
}
