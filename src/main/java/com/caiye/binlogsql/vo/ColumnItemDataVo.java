package com.caiye.binlogsql.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@SuppressWarnings("AlibabaClassMustHaveAuthor")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ColumnItemDataVo {

    private Object value;
    private ColumnVo column;
}
