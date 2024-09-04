package com.tapdata.tm.base.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.TmPageable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * @author samuel
 * @Description
 * @create 2024-09-04 16:28
 **/
@DisplayName("Class BaseService Test")
class BaseServiceTest {

	private BaseService<?, ?, ?, ?> baseService;

	@BeforeEach
	void setUp() {
		baseService = Mockito.mock(BaseService.class);
	}

	@Nested
	@DisplayName("Method filterToTmPageable test")
	class filterToTmPageableTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			Filter filter = new Filter();
			filter.setSkip(21);
			filter.setLimit(10);
			when(baseService.filterToTmPageable(any())).thenCallRealMethod();
			TmPageable tmPageable = baseService.filterToTmPageable(filter);
			assertEquals(3, tmPageable.getPage());
			assertEquals(10, tmPageable.getSize());
		}
	}
}