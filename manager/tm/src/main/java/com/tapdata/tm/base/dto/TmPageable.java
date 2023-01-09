package com.tapdata.tm.base.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.io.Serializable;

@AllArgsConstructor
@Getter
@Setter
public class TmPageable implements Serializable, Pageable {
    // 当前页,默认取第一页
    @Min(1)
    private Integer page;
    // 当前页面条数
    @Max(100)
    @Min(10)
    private Integer size;
    //排序条件
    private Sort sort = Sort.by("createTime").descending();

    @Min(0)
    private Integer offset;

    public TmPageable() {

    }

    public TmPageable(int offset, int size, Sort sort) {
        this.size = size;
        this.offset = offset;

        this.sort = sort;
    }

    public void setSort(Sort sort) {
        this.sort = sort;
    }



    public TmPageable(Integer page, Integer size) {
        this.page = page;
        this.size = size;
    }


    // 当前页面
    @Override
    public int getPageNumber() {
        return page;
    }

    // 每一页显示的条数
    @Override
    public int getPageSize() {
        return size;
    }

    // 第二页所需要增加的数量
    @Override
    public long getOffset() {
        if (offset != null) {
            return offset;
        }
        return (page - 1) * size.longValue();
    }

    @Override
    public Sort getSort() {
        return sort;
    }

    @Override
    public Pageable next() {
        return null;
    }

    @Override
    public Pageable previousOrFirst() {
        return null;
    }

    @Override
    public Pageable first() {
        return null;
    }

    @Override
    public Pageable withPage(int pageNumber) {
        return null;
    }

    @Override
    public boolean hasPrevious() {
        return false;
    }
}
