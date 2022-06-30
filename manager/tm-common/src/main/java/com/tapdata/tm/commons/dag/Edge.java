package com.tapdata.tm.commons.dag;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/11/3 下午3:22
 * @description
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class Edge extends Element {

    private String source;
    private String target;

    public Edge(){
        super(ElementType.Link);
    }

    public Edge(String source, String target){
        super(ElementType.Link);
        this.source = source;
        this.target = target;
    }
}
