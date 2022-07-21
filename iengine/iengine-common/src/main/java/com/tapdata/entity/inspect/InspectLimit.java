package com.tapdata.entity.inspect;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/7 10:23 上午
 * @description
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class InspectLimit implements Serializable {

	private int keep = 1000;
	private int fullMatchKeep = 100;
	private String action = "stop";

}
