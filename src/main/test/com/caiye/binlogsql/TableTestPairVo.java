package com.caiye.binlogsql;

import lombok.AllArgsConstructor;
import lombok.Data;

@SuppressWarnings({"AlibabaClassMustHaveAuthor", "WeakerAccess"})
@Data
@AllArgsConstructor
public class TableTestPairVo {

    private String orgTableName;
    private String testTableName;

    public String replaceTableName(String sql) {
        return sql.replace("`" + orgTableName + "`", "`" + testTableName + "`");
    }
}
