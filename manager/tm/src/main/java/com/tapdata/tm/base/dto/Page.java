package com.tapdata.tm.base.dto;

import lombok.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/11 2:58 下午
 * @description
 */
@Getter
@Setter
@EqualsAndHashCode
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Page<T>{

    protected long total;
    protected List<T> items;

}
