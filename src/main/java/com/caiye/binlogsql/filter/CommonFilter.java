package com.caiye.binlogsql.filter;

import com.caiye.binlogsql.vo.TableVo;
import com.github.shyiko.mysql.binlog.event.Event;
import com.caiye.binlogsql.vo.FilterDbTableVo;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;

@SuppressWarnings("AlibabaClassMustHaveAuthor")
@Data
@Accessors(chain = true)
@Slf4j
public class CommonFilter implements Filter {

    /** 过滤schema 或者 table */
    private List<FilterDbTableVo> includeDbTableVos;
    /** 过滤日志事件 */
    private List<EventFilter> eventFilters;
    /** 过滤生成的sql */
    private List<SqlFilter> sqlFilters;

    @Override
    public boolean filter(String schema) {
        if (includeDbTableVos == null) {
            return true;
        }
        if (includeDbTableVos.isEmpty()) {
            log.warn("没有设置监听的数据库");
            return false;
        }
        for (FilterDbTableVo includeDbTableVo : includeDbTableVos) {
            if (Objects.equals(schema, includeDbTableVo.getDbName())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean filter(TableVo tableVoInfo) {
        if (includeDbTableVos == null) {
            return true;
        }
        if (includeDbTableVos.isEmpty()) {
            log.warn("没有设置监听的数据库和表");
            return false;
        }
        for (FilterDbTableVo includeDbTableVo : includeDbTableVos) {
            if (Objects.equals(tableVoInfo.getDbName(), includeDbTableVo.getDbName()) &&
                    Objects.equals(tableVoInfo.getTableName(), includeDbTableVo.getTableName())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean filter(Event event) {
        if (eventFilters != null && eventFilters.size() > 0) {
            return eventFilters.stream().anyMatch(eventFilter -> eventFilter.test(event));
        }
        return true;
    }

    @Override
    public boolean sqlFilter(String sql) {
        if (sqlFilters != null && sqlFilters.size() > 0) {
            return sqlFilters.stream().anyMatch(sqlFilter -> sqlFilter.test(sql));
        }
        return true;
    }
}
