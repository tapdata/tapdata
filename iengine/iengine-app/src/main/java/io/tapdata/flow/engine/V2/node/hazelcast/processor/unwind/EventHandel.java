package io.tapdata.flow.engine.V2.node.hazelcast.processor.unwind;

import com.tapdata.tm.commons.dag.process.UnwindProcessNode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author GavinXiao
 * @description EventHandel create by Gavin
 * @create 2023/10/8 18:01
 * @doc https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/
 **/
public interface EventHandel {
    Map<String, EventHandel> handelMap = new ConcurrentHashMap<>();

    static List<TapEvent> getHandelResult(UnwindProcessNode node, TapEvent event) {
        final String op = event instanceof TapUpdateRecordEvent ? "u" :
                    event instanceof TapInsertRecordEvent ? "i" :
                    event instanceof TapDeleteRecordEvent ? "d" : null;
        if (null == op) return null;
        EventHandel eventHandel = handelMap.get(op);
        if (null == eventHandel) {
            switch (op) {
                case "u" : eventHandel = new UpdateHandel();break;
                case "i" : eventHandel = new InsertHandel();break;
                default: eventHandel = new DeleteHandel();
            }
            handelMap.put(op, eventHandel);
        }


        List<TapEvent> events = UnWindNodeUtil.initHandel(node, event);
        if (!events.isEmpty()) return events;
        return eventHandel.handel(node, event);
    }

    static void close() {
        handelMap.clear();
    }

    List<TapEvent> handel(UnwindProcessNode node, TapEvent event);

    void copyEvent(List<TapEvent> events, Map<String, Object> item, TapEvent event);
}

class InsertHandel implements EventHandel {
    @Override
    public List<TapEvent> handel(UnwindProcessNode node, TapEvent event) {
        return UnWindNodeUtil.handelList(node, event, UnWindNodeUtil.getAfter(event), this);
    }

    @Override
    public void copyEvent(List<TapEvent> events, Map<String, Object> item, TapEvent event) {
        TapInsertRecordEvent e = TapInsertRecordEvent.create();
        e.after(item);
        e.setReferenceTime(((TapInsertRecordEvent)event).getReferenceTime());
        e.setTableId(((TapInsertRecordEvent)event).getTableId());
        events.add(e);
    }
}

class DeleteHandel implements EventHandel {
    @Override
    public List<TapEvent> handel(UnwindProcessNode node, TapEvent event) {
        return UnWindNodeUtil.handelList(node, event, UnWindNodeUtil.getBefore(event), this);
    }

    @Override
    public void copyEvent(List<TapEvent> events, Map<String, Object> item, TapEvent event) {
        TapDeleteRecordEvent e = TapDeleteRecordEvent.create();
        e.before(item);
        e.setReferenceTime(((TapDeleteRecordEvent)event).getReferenceTime());
        e.setTableId(((TapDeleteRecordEvent)event).getTableId());
        events.add(e);
    }
}

class UpdateHandel implements EventHandel {

    @Override
    public void copyEvent(List<TapEvent> events, Map<String, Object> item, TapEvent event) {
        TapUpdateRecordEvent e = TapUpdateRecordEvent.create();
        e.after(item);
        e.setReferenceTime(((TapUpdateRecordEvent) event).getReferenceTime());
        e.setTableId(((TapUpdateRecordEvent)event).getTableId());
        events.add(e);
    }

    @Override
    public List<TapEvent> handel(UnwindProcessNode node, TapEvent event){
        List<TapEvent> events = new ArrayList<>();
        Map<String, Object> after = UnWindNodeUtil.getAfter(event);
        Map<String, Object> before = UnWindNodeUtil.getBefore(event);
        if (null == before || before.isEmpty()) {
            return UnWindNodeUtil.handelList(node, event, after, this);
        } else {
            Long referenceTime = ((TapUpdateRecordEvent) event).getReferenceTime();
            TapDeleteRecordEvent delete = TapDeleteRecordEvent.create();
            delete.before(before);
            delete.referenceTime(referenceTime);
            List<TapEvent> deletes = EventHandel.getHandelResult(node, delete);
            if (null != deletes) {
                events.addAll(deletes);
            }
            TapInsertRecordEvent insert = TapInsertRecordEvent.create();
            insert.after(after);
            insert.referenceTime(referenceTime);
            List<TapEvent> inserts = EventHandel.getHandelResult(node, insert);
            if (null != inserts) {
                events.addAll(inserts);
            }
        }
        return events;
    }
}