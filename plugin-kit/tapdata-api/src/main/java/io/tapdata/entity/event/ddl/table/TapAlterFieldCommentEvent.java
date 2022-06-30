package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.util.ArrayList;
import java.util.List;

public class TapAlterFieldCommentEvent extends TapTableEvent {
    public static final int TYPE = 300;
    private List<FieldAttrChange<String>> commentChanges;
    public TapAlterFieldCommentEvent change(FieldAttrChange<String> change) {
        if(commentChanges == null)
            commentChanges = new ArrayList<>();
        if(change != null && commentChanges.contains(change))
            commentChanges.add(change);
        return this;
    }
    public TapAlterFieldCommentEvent() {
        super(TYPE);
    }
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapAlterFieldCommentEvent) {
            TapAlterFieldCommentEvent alterFieldCommentEvent = (TapAlterFieldCommentEvent) tapEvent;
            if (commentChanges != null)
                alterFieldCommentEvent.commentChanges = new ArrayList<>(commentChanges);
        }
    }

    public List<FieldAttrChange<String>> getCommentChanges() {
        return commentChanges;
    }

    public void setCommentChanges(List<FieldAttrChange<String>> commentChanges) {
        this.commentChanges = commentChanges;
    }
}
