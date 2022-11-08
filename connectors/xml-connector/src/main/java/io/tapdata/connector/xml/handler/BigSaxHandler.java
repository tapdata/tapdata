package io.tapdata.connector.xml.handler;

import org.dom4j.Element;
import org.dom4j.ElementHandler;
import org.dom4j.ElementPath;

public class BigSaxHandler implements ElementHandler {

    private final String path;

    public BigSaxHandler(String path) {
        this.path = path;
    }

    @Override
    public void onStart(ElementPath elementPath) {

    }

    @Override
    public void onEnd(ElementPath elementPath) {
        Element element = elementPath.getCurrent();
        if (elementPath.getPath().equals(path) || elementPath.getPath().startsWith(path)) {
            System.out.println(element.getData());
        }
        element.detach();
    }
}
