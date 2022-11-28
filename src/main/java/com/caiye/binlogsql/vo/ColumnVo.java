package com.caiye.binlogsql.vo;

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.JDBCType;

@SuppressWarnings("AlibabaClassMustHaveAuthor")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ColumnVo {

    private String name;
    private ColumnType columnType;
    private JDBCType jdbcType;
}
