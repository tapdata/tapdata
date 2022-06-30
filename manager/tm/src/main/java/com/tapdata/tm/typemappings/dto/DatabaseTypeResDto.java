/**
 * @title: DatabaseTypeResDto
 * @description:
 * @author lk
 * @date 2021/9/14
 */
package com.tapdata.tm.typemappings.dto;

import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DatabaseTypeResDto {

	private String dbType;

	private List<Rule> rules;

	@Getter
	@Setter
	public static class Rule{

		private Long minPrecision;

		private Long maxPrecision;

		private Long minScale;

		private Long maxScale;
	}
}
