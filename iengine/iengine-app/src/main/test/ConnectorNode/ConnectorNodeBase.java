package ConnectorNode;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import org.junit.Before;

import java.lang.reflect.Field;

public class ConnectorNodeBase {

    protected static ConnectorNode sqlConnectorNode = null;
    protected static TapTable myTapTable = null;

    protected static ConnectorNode mongoConnectorNode = null;

    @Before
    public void initSqlConnectorNode() throws NoSuchFieldException, IllegalAccessException {
        ConnectorNode connectorNode = new ConnectorNode();
        TapNodeInfo tapNodeInfo = new TapNodeInfo();
        TapNodeSpecification tapNodeSpecification = new TapNodeSpecification();
        tapNodeSpecification.setId("mysql");
        tapNodeInfo.setTapNodeSpecification(tapNodeSpecification);
        Class clazz = ConnectorNode.class;
        Class<?> superClass = clazz.getSuperclass();
        Field field_name = superClass.getDeclaredField("tapNodeInfo");
        field_name.setAccessible(true);
        field_name.set(connectorNode, tapNodeInfo);
        sqlConnectorNode = connectorNode;

        TapTable table = new TapTable();
        table.setId("testID");
        table.setName("testID");
        myTapTable = table;
    }

    @Before
    public void initMongodbConnectorNode() throws NoSuchFieldException, IllegalAccessException {
        ConnectorNode connectorNode = new ConnectorNode();
        TapNodeInfo tapNodeInfo = new TapNodeInfo();
        TapNodeSpecification tapNodeSpecification = new TapNodeSpecification();
        tapNodeSpecification.setId("mongodb");
        tapNodeInfo.setTapNodeSpecification(tapNodeSpecification);
        Class clazz = ConnectorNode.class;
        Class<?> superClass = clazz.getSuperclass();
        Field field_name = superClass.getDeclaredField("tapNodeInfo");
        field_name.setAccessible(true);
        field_name.set(connectorNode, tapNodeInfo);
        mongoConnectorNode = connectorNode;
    }
}
