package io.tapdata.connector.xml.handler;

import io.tapdata.exception.StopException;
import org.dom4j.Element;
import org.dom4j.ElementHandler;
import org.dom4j.ElementPath;
import org.dom4j.Node;
import org.dom4j.tree.DefaultElement;
import org.dom4j.tree.DefaultText;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BigSaxSchemaHandler implements ElementHandler {

    private String path;
    private Map<String, Object> sampleResult;

    public BigSaxSchemaHandler() {

    }

    public String getPath() {
        return path;
    }

    public BigSaxSchemaHandler withPath(String path) {
        this.path = path;
        return this;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Map<String, Object> getSampleResult() {
        return sampleResult;
    }

    public BigSaxSchemaHandler withSampleResult(Map<String, Object> sampleResult) {
        this.sampleResult = sampleResult;
        return this;
    }

    @Override
    public void onStart(ElementPath elementPath) {

    }

    @Override
    public void onEnd(ElementPath elementPath) {
        Element element = elementPath.getCurrent();
        if (path.equals(elementPath.getPath())) {
            Object res = analyzeElement(element);
            if (res instanceof Map) {
                sampleResult.putAll((Map) res);
            } else {
                sampleResult.put(element.getName(), res);
            }
            throw new StopException();
        }
        if (!elementPath.getPath().startsWith(path) || path.equals(elementPath.getPath())) {
            element.detach();
        }
    }

    private Object analyzeElement(Element element) {
        List<Node> nodes = element.content();
        if (nodes.size() == 1 && nodes.get(0) instanceof DefaultText) {
            return nodes.get(0).getText();
        } else {
            List<Node> newNodes = nodes.stream().filter(v -> v instanceof DefaultElement).collect(Collectors.toList());
            if (newNodes.stream().map(Node::getPath).distinct().count() > 1) {
                Map<String, Object> subMap = new LinkedHashMap<>();
                newNodes.forEach(v -> subMap.put(v.getName(), analyzeElement((DefaultElement) v)));
                return subMap;
            } else {
                List<Object> subList = new ArrayList<>();
                newNodes.forEach(v -> subList.add(analyzeElement((DefaultElement) v)));
                return subList;
            }
        }
    }
}
