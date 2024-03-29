package com.tapdata.tm.base.dto;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/8/4 7:13 上午
 * @description
 */
@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class Field extends HashMap<String, Object> {

}
