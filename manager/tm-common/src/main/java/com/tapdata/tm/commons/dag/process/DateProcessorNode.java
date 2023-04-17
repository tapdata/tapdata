package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.NodeType;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;


@NodeType("date_processor")
@Getter
@Setter
@Slf4j
public class DateProcessorNode extends ProcessorNode {
    /** 需要修改时间的类型 */
    private List<String> dataTypes;
    /** 增加或者减少 */
    private boolean add;
    /** 增加或者减少的小时数 */
    private int hours;
    public DateProcessorNode() {
        super("date_processor");
    }
}
