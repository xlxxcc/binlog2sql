package com.caiye.binlogsql.handler;

import com.github.shyiko.mysql.binlog.event.Event;

import java.util.List;

@SuppressWarnings("AlibabaClassMustHaveAuthor")
public interface BinlogEventHandler {

    /**
     * 处理日志事件
     * @param event 日志事件
     * @param isTurn true:返回回滚sql, false:返回提交sql
     * @return sqlList
     */
    List<String> handle(Event event, boolean isTurn) ;
}
