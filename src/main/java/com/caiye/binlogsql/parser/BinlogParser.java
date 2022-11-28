package com.caiye.binlogsql.parser;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.caiye.binlogsql.filter.Filter;
import com.caiye.binlogsql.handler.BinlogEventHandler;
import com.caiye.binlogsql.tool.FileTool;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"WeakerAccess", "AlibabaClassMustHaveAuthor", "unused"})
@Slf4j
public class BinlogParser {

    @Getter
    private Map<EventType, BinlogEventHandler> handleRegisterMap = new HashMap<>();

    @Setter
    private Filter filter = new Filter() {
    };

    public void registerHandle(BinlogEventHandler handle, EventType... eventTypes) {
        for (EventType eventType : eventTypes) {
            handleRegisterMap.put(eventType, handle);
        }
    }

    /**
     * @param event: binlog event
     */
    public void handle(Event event) {
        handle(event, false);
    }

    /**
     * @param event: binlog event
     * @param isTurn: 是否返回回滚sql
     */
    public void handle(Event event, boolean isTurn) {
        handle(event, isTurn, "sql.sql");
    }

    /**
     * @param event: binlog event
     * @param isTurn: 是否返回回滚sql
     * @param fileName: 输出文件
     */
    public void handle(Event event, boolean isTurn, String fileName) {
        BinlogEventHandler binlogEventHandle = handleRegisterMap.get(event.getHeader().getEventType());
        if (binlogEventHandle != null) {
            List<String> sqls = binlogEventHandle.handle(event, isTurn);
            //noinspection AlibabaAvoidComplexCondition
            if (!sqls.isEmpty()) {
                sqls.forEach(sql -> {
                    if (filter == null || filter.sqlFilter(sql)) {
                        try {
                            FileTool.appendFileContent(fileName, sql);
                        } catch (IOException e) {
                            log.error(e.getMessage(), e);
                        }
                        log.info("handle sql: {};", sql);
                    }
                });
            }
        }
    }
}
