package com.tapdata.tm.base.dto;

import lombok.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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

    public static <T> Page<T> page(List<T> list, Number total) {
        Page<T> page = new Page<>();
        page.setItems(Optional.ofNullable(list).orElse(new ArrayList<>()));
        page.setTotal(Optional.ofNullable(total).map(Number::longValue).orElse(0L));
        return page;
    }

    public static <T> Page<T> empty() {
        return Page.page(new ArrayList<>(), 0L);
    }
}
