package io.tapdata.common.support.comom;

import java.util.List;

public class TapApiBase {

    List<String> tapTables;

    public List<String> tapTable(){
        return this.tapTables;
    }

    public void tables(List<String> tapTables){
        this.tapTables = tapTables;
    }

}
