package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.script.MigrateScriptProcessNode;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class MigrateScriptProcessNodeTest {

    @Nested
    class AddCrateTapFieldTest{
        private MigrateScriptProcessNode migrateScriptProcessNode;
        @BeforeEach
        void beforeEach(){
            migrateScriptProcessNode=new MigrateScriptProcessNode("processor",Node.NodeCatalog.processor);
        }
        @Test
        void testAddCrateTapFieldNullMap(){
            TapTable table = new TapTable("test1","test1Id");
            migrateScriptProcessNode.addCrateTapField(null, table);
            assertNull(table.getNameFieldMap());
        }
        @Test
        void testAddCrateTapFieldCreateMap(){
            String dataType = "string";
            String tapFieldName = "name1";
            Map<String, TapField> createTapFieldMap = new HashMap<>();
            createTapFieldMap.put("create", new TapField(tapFieldName, dataType));
            TapTable table = new TapTable("test1", "testId");
            migrateScriptProcessNode.addCrateTapField(createTapFieldMap, table);
            assertNotNull(table.getNameFieldMap());
            LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
            TapField tapField = nameFieldMap.get(tapFieldName);
            assertNotNull(tapField);
            assertEquals(dataType, tapField.getDataType());
        }
    }
}
