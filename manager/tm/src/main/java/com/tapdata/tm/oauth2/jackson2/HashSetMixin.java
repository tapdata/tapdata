package com.tapdata.tm.oauth2.jackson2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Set;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/24 下午7:58
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
abstract class HashSetMixin {

    @JsonCreator
    HashSetMixin(Set<?> set) {
    }

}

