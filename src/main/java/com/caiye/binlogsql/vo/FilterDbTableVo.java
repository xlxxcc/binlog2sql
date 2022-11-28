package com.caiye.binlogsql.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@SuppressWarnings("AlibabaClassMustHaveAuthor")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FilterDbTableVo {

    private String dbName;
    private String tableName;
}
