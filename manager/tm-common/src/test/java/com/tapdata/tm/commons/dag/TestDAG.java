//package com.tapdata.tm.dag;
//
//import com.tapdata.tm.commons.dag.DAG;
//import com.tapdata.tm.commons.schema.Field;
//import com.tapdata.tm.commons.schema.Schema;
//import org.junit.Assert;
//import org.junit.Test;
//
//import java.util.ArrayList;
//
///**
// * @author lg&lt;lirufei0808@gmail.com&gt;
// * create at 2021/11/9 下午3:41
// */
//public class TestDAG {
//
//    @Test
//    public void testLoader() {
//        Assert.assertTrue(DAG.nodeMapping.size() > 0);
//        Assert.assertTrue(DAG.nodeMapping.containsKey("table"));
//        Assert.assertTrue(DAG.nodeMapping.containsKey("database"));
//    }
//
//    @Test
//    public void testSchemaEquals() {
//        Schema schema = new Schema();
//        schema.setFields(new ArrayList<Field>(){{
//            Field field = new Field();
//            field.setFieldName("test");
//            add(field);
//        }});
//
//        Schema schema1 = new Schema();
//        schema1.setFields(new ArrayList<Field>(){{
//            Field field = new Field();
//            field.setFieldName("test");
//            add(field);
//        }});
//
//        Assert.assertEquals(schema.hashCode(), schema1.hashCode());
//        Assert.assertEquals(schema.toString(), schema1.toString());
//
//        schema1.setOriginalName("test");
//        Assert.assertNotEquals(schema, schema1);
//    }
//
//}
