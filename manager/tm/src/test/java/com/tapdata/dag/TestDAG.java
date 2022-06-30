//package com.tapdata.dag;
//
//import com.tapdata.tm.commons.dag.DAG;
//import com.tapdata.tm.dag.util.DAGUtils;
//import com.tapdata.tm.commons.dag.vo.FieldProcess;
//import com.tapdata.tm.dataflow.dto.Stage;
//import org.junit.jupiter.api.Test;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.UUID;
//
///**
// * @author lg<lirufei0808 @ gmail.com>
// * @date 2021/11/5 上午11:18
// * @description
// */
//public class TestDAG {
//
//    @Test
//    public void testBuildDAGByStage() {
//        List<Stage> stages = new ArrayList<>();
//
//        Stage source = new Stage();
//        source.setId(UUID.randomUUID().toString());
//        source.setType("table");
//        source.setTableName("source");
//        source.setConnectionId("test connection id");
//
//        stages.add(source);
//
//        Stage fieldProcessor = new Stage();
//        fieldProcessor.setId(UUID.randomUUID().toString());
//        fieldProcessor.setType("field_processor");
//        fieldProcessor.setOperations(new ArrayList<FieldProcess>(){{
//            FieldProcess fieldProcessor = new FieldProcess();
//            /*fieldProcessor.setField("a");
//            fieldProcessor.setJavaType("String");
//            fieldProcessor.setOp("RENAME");
//            fieldProcessor.setOperand("b");
//            fieldProcessor.setOriginalDataType("Int");
//            fieldProcessor.setOriginedatatype("Int");*/
//            add(fieldProcessor);
//        }});
//        stages.add(fieldProcessor);
//
//        Stage target = new Stage();
//        target.setId(UUID.randomUUID().toString());
//        target.setType("table");
//        target.setTableName("target");
//        target.setConnectionId("test connection id");
//
//        stages.add(target);
//
//        source.setOutputLanes(new ArrayList<String>(){{
//            add(fieldProcessor.getId());
//        }});
//        fieldProcessor.setInputLanes(new ArrayList<String>(){{
//            add(source.getId());
//        }});
//        fieldProcessor.setOutputLanes(new ArrayList<String>(){{
//            add(target.getId());
//        }});
//        target.setInputLanes(new ArrayList<String>(){{
//            add(fieldProcessor.getId());
//        }});
//
//        DAG dag = DAGUtils.build("test user id", null, stages, null);
//        dag.transformSchema(source.getId(), null);
//
//    }
//}
