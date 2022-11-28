package com.caiye.binlogsql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@SuppressWarnings({"AlibabaClassMustHaveAuthor", "WeakerAccess"})
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SqlTestVo {

    private List<String> sqls;
    private List<String> rollSqls;
}
