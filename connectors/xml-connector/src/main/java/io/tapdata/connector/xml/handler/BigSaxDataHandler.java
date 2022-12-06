package io.tapdata.connector.xml.handler;

import io.tapdata.common.FileOffset;
import io.tapdata.common.util.MatchUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.exception.StopException;
import io.tapdata.kit.EmptyKit;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.insertRecordEvent;
import static io.tapdata.entity.simplify.TapSimplify.list;

public class BigSaxDataHandler implements ElementHandler {

    private String path;
    private FileOffset fileOffset;
    private TapTable tapTable;
    private int eventBatchSize;
    private BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer;
    private AtomicReference<List<TapEvent>> tapEvents;
    private Supplier<Boolean> isAlive;
    private Map<String, String> dataTypeMap;
    private long lastModified;
    private int blankSkip = 0;

    public BigSaxDataHandler() {

    }

    public String getPath() {
        return path;
    }

    public BigSaxDataHandler withLastModified(long lastModified) {
        this.lastModified = lastModified;
        return this;
    }

    public BigSaxDataHandler withPath(String path) {
        this.path = path;
        return this;
    }

    public BigSaxDataHandler withFlag(Supplier<Boolean> isAlive) {
        this.isAlive = isAlive;
        return this;
    }

    public BigSaxDataHandler withConfig(FileOffset fileOffset, TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer, AtomicReference<List<TapEvent>> tapEvents) {
        this.fileOffset = fileOffset;
        this.tapTable = tapTable;
        this.dataTypeMap = tapTable.getNameFieldMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().getDataType()));
        this.eventBatchSize = eventBatchSize;
        this.eventsOffsetConsumer = eventsOffsetConsumer;
        this.tapEvents = tapEvents;
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
            DataMap dataMap = new DataMap();
            if (res instanceof Map) {
                dataMap.putAll((Map) res);
            } else {
                dataMap.put(element.getName(), res);
            }
            if (dataMap.entrySet().stream().allMatch(v -> EmptyKit.isNull(v.getValue()))) {
                blankSkip++;
                return;
            }
            tapEvents.get().add(insertRecordEvent(dataMap, tapTable.getId()).referenceTime(lastModified));
            if (tapEvents.get().size() == eventBatchSize) {
                fileOffset.setDataLine(fileOffset.getDataLine() + eventBatchSize + blankSkip);
                blankSkip = 0;
                fileOffset.setPath(fileOffset.getPath());
                eventsOffsetConsumer.accept(tapEvents.get(), fileOffset);
                tapEvents.set(list());
            }
        }
        if (!elementPath.getPath().startsWith(path) || path.equals(elementPath.getPath())) {
            element.detach();
        }
        if (!isAlive.get()) {
            throw new StopException();
        }
    }

    private Object analyzeElement(Element element) {
        List<Node> nodes = element.content();
        if (nodes.size() == 1 && nodes.get(0) instanceof DefaultText) {
            try {
                return MatchUtil.parse(nodes.get(0).getText(), dataTypeMap.get(element.getName()));
            } catch (Exception e) {
                throw new RuntimeException(String.format("%s field has invalid value", element.getName()), e);
            }
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
