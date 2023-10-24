package io.tapdata.supervisor.convert.entity;

import java.util.Map;

class WCode extends WBase implements Resolvable<WCode> {
    protected String type;
    protected String line;
    private WCodeAgent<?> agent;
    private boolean needCreate;
    protected boolean finallyIs;
    protected boolean redundantIs;
    protected WException exception;
    protected int index;
    protected boolean appendIs;

    @Override
    public WCode parser(Map<String, Object> parserMap) {
        super.parser(parserMap);
        if (this.ignore) return null;
        this.type = WZTags.toString(parserMap, WZTags.W_TYPE, WZTags.DEFAULT_EMPTY);
        this.line = WZTags.toString(parserMap, WZTags.W_LINE, WZTags.DEFAULT_EMPTY);
        this.needCreate = WZTags.toBoolean(parserMap, WZTags.W_IS_CREATE, Boolean.FALSE);
        this.finallyIs = WZTags.toBoolean(parserMap, WZTags.W_IS_FINALLY, false);
        this.redundantIs = WZTags.toBoolean(parserMap, WZTags.W_IS_REDUNDANT, false);
        this.exception = new WException().parser(WZTags.toMap(parserMap, WZTags.CODE_EXCEPTION));
        this.index = WZTags.toInt(parserMap, WZTags.W_LINE_INDEX, WZTags.DEFAULT_ONE);
        this.appendIs = WZTags.toBoolean(parserMap, WZTags.W_IS_APPEND, false);
        WCode target;
        switch (this.type) {
            case WZTags.CODE_AFTER:
                target = new WCodeAfter();
                break;
            case WZTags.CODE_NORMAL:
                target = new WCodeNormal();
                break;
            case WZTags.CODE_CATCH:
                target = new WCodeCatch();
                break;
            default:
                target = new WCodeBefore();
        }
        this.parser(target);
        return target;
    }

    public void parser(WCode target) {
        target.setType(this.type);
        target.setLine(this.line);
        target.setNeedCreate(this.needCreate);
        target.setFinallyIs(this.finallyIs);
        target.setRedundantIs(this.redundantIs);
        target.setException(this.exception);
        target.setIndex(this.index);
        target.setAppendIs(this.appendIs);
    }

    public boolean isNeedCreate() {
        return needCreate;
    }

    public void setNeedCreate(boolean needCreate) {
        this.needCreate = needCreate;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getLine() {
        return line;
    }

    public WCodeAgent<?> getAgent() {
        return agent;
    }

    public void setAgent(WCodeAgent<?> agent) {
        this.agent = agent;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public boolean isFinallyIs() {
        return finallyIs;
    }

    public void setFinallyIs(boolean finallyIs) {
        this.finallyIs = finallyIs;
    }

    public boolean isRedundantIs() {
        return redundantIs;
    }

    public void setRedundantIs(boolean redundantIs) {
        this.redundantIs = redundantIs;
    }

    public WException getException() {
        return exception;
    }

    public void setException(WException exception) {
        this.exception = exception;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public boolean isAppendIs() {
        return appendIs;
    }

    public void setAppendIs(boolean appendIs) {
        this.appendIs = appendIs;
    }
}
