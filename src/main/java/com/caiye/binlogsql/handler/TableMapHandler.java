package com.caiye.binlogsql.handler;

import com.caiye.binlogsql.tool.TableTool;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.caiye.binlogsql.filter.Filter;
import com.caiye.binlogsql.vo.DbInfoVo;

import java.util.Collections;
import java.util.List;

import static com.caiye.binlogsql.tool.TableTool.setTableInfo;

@SuppressWarnings({"Duplicates", "AlibabaClassMustHaveAuthor"})
public class TableMapHandler implements BinlogEventHandler {

    private final Filter filter;

    private final DbInfoVo dbInfoVo;

    public TableMapHandler(Filter filter, DbInfoVo dbInfoVo) {
        this.filter = filter;
        this.dbInfoVo = dbInfoVo;
    }


    @Override
    public List<String> handle(Event event, boolean isTurn) {
        TableMapEventData queryEventData = event.getData();
        if (!filter.filter(queryEventData.getDatabase())) {
            TableTool.setTableInfo(queryEventData);
        } else {
            TableTool.setTableInfo(dbInfoVo, queryEventData);
        }
        return Collections.emptyList();
    }

}
