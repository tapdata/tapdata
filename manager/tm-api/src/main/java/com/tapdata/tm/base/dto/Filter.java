package com.tapdata.tm.base.dto;

import com.tapdata.manager.common.utils.StringUtils;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


import java.util.ArrayList;
import java.util.List;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/11 3:00 下午
 * @description
 */
@Getter
@Setter
@EqualsAndHashCode
@ToString
public class Filter {

	@Min(1)
	@Parameter(description = "One-based page number.", schema = @Schema(type = "integer", defaultValue = "1"))
	private int page = 1;

	@Min(0)
	@Parameter(description = "Page size.", schema = @Schema(type = "integer", defaultValue = "20"))
	private int size = 20;

	@Parameter(description = "Sorting criteria in the format: property( asc|desc). Default sort order is ascending.")
	private List<String> sort = new ArrayList<>();

	@Parameter(description = "Conditions for querying data, see <a href='https://docs.mongodb.com/manual/tutorial/query-documents/'>https://docs.mongodb.com/manual/tutorial/query-documents/</a> for more details.")
	private Where where;

	@Parameter(description = "Project fields to return from query.")
	private Field fields;

	@Min(0)
	@Parameter(description = "Zero-based skip documents (0..N)", schema = @Schema(type = "integer", defaultValue = "0"))
	private int skip = 0;

	@Min(0)
	@Parameter(description = "Zero-based limit documents (0..N)", schema = @Schema(type = "integer", defaultValue = "0"))
	private int limit = 0;

	@Parameter(description = "Sorting criteria in the format: property( asc|desc). Default sort order is ascending.")
	private Object order;

	public Filter(){
		this.where = new Where();
	}

	public Filter(Where where) {
		this();
		this.where = where;
	}

	/**
	 * Instantiates a new filter with explicit pagination defaults.
	 *
	 * @param page the page
	 * @param size the size
	 * @param sort the sort
	 */
	public Filter(int page, int size, List<String> sort) {
		this.page = page;
		this.size = size;
		this.sort = sort;
	}

	public Where where(String prop, Object value) {
		if (this.where == null) {
			this.where = new Where();
		}
		return this.where.and(prop, value);
	}

	public int getSkip() {
		return skip > 0 ? skip : (getPage() - 1) * getSize();
	}

	public int getLimit() {
		return limit > 0 ? limit : getSize();
	}

	public List<String> getSort() {
		List<String> sort = this.sort == null ? new ArrayList<>() : new ArrayList<>(this.sort);
		if (order != null) {
			if (order instanceof String) {
				if (StringUtils.isNotBlank((String)order)) {
					sort.add((String)order);
				}
			} else if (order instanceof ArrayList) {
				sort.addAll((ArrayList)order);
			}
		}
		return sort;
	}
}
