package com.caiye.binlogsql.filter;

import com.caiye.binlogsql.vo.TableVo;
import com.github.shyiko.mysql.binlog.event.Event;

@SuppressWarnings("AlibabaClassMustHaveAuthor")
public interface Filter {

    /**
     * 过滤schema, handler filter
     *
     * @param schema schema
     * @return boolean
     */
    default boolean filter(String schema) {
        return true;
    }

    /**
     * 过滤table, handler filter
     *
     * @param tableVoInfo schema
     * @return boolean
     */
    default boolean filter(TableVo tableVoInfo) {
        return true;
    }

    /**
     * 过滤event, parser event filter
     *
     * @param event schema
     * @return boolean
     */
    default boolean filter(Event event) {
        return true;
    }

    /**
     * 过滤sql, parser sql filter
     *
     * @param sql sql
     * @return boolean
     */
    default boolean sqlFilter(String sql) {
        return true;
    }
}
