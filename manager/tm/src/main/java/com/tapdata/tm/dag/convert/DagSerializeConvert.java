package com.tapdata.tm.dag.convert;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.util.JsonUtil;
import org.apache.commons.collections.CollectionUtils;
import org.bson.Document;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;

import java.util.List;


/**
 * @Author: Zed
 * @Date: 2021/11/6
 * @Description:
 */
@WritingConverter
public class DagSerializeConvert implements Converter<DAG, Document> {
    @Override
    public Document convert(DAG dag) {
        List<Node> nodes = dag.getNodes();
        if (CollectionUtils.isNotEmpty(nodes)) {
            nodes.forEach(node -> {
                node.setSchema(null);
                node.setOutputSchema(null);
            });
        }
        Dag dag1 = dag.toDag();
        String json = JsonUtil.toJsonUseJackson(dag1);
        return Document.parse(json);
    }
}
