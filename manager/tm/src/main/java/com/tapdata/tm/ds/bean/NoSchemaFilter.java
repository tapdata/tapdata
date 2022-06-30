package com.tapdata.tm.ds.bean;

import com.tapdata.tm.base.dto.Filter;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/11 3:00 下午
 * @description
 */
@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
@ToString
public class NoSchemaFilter extends Filter {
	private Integer noSchema;
}
