package io.tapdata.ConnectorNode;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;

import java.lang.reflect.Field;

public class ConnectorNodeBase {

    protected  ConnectorNode sqlConnectorNode = null;
    protected static TapTable myTapTable = null;

    protected static ConnectorNode mongoConnectorNode = null;


    public void initConnectorNode(ConnectorNode connectorNode,TapNodeSpecification tapNodeSpecification) throws NoSuchFieldException, IllegalAccessException {
        TapNodeInfo tapNodeInfo = new TapNodeInfo();
        tapNodeInfo.setTapNodeSpecification(tapNodeSpecification);
        invokeValueForFiled(ConnectorNode.class,"tapNodeInfo",connectorNode,tapNodeInfo,true);
    }




    public void invokeValueForFiled(Class clazz, String filedName, ConnectorNode connectorNode, Object object, boolean superClass) throws NoSuchFieldException, IllegalAccessException {
        Class<?> superClassTemp;
        if (superClass) {
            superClassTemp = clazz.getSuperclass();
        } else {
            superClassTemp = clazz;
        }
        Field field_name = superClassTemp.getDeclaredField(filedName);
        field_name.setAccessible(true);
        field_name.set(connectorNode, object);
    }
}
