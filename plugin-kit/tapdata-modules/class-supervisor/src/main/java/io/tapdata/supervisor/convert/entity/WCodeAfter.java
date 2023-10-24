package io.tapdata.supervisor.convert.entity;

import java.util.Map;

class WCodeAfter extends WCode{

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

    @Override
    public WCodeAfter parser(Map<String, Object> parserMap) {
        super.parser(parserMap);
        return this;
    }

}
