package com.caiye.binlogsql.vo;

import lombok.Data;

import java.util.List;

@SuppressWarnings("AlibabaClassMustHaveAuthor")
@Data
public class RowVo {
    private List<ColumnItemDataVo> value;
}
