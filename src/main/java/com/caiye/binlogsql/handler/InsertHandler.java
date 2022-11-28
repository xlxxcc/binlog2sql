package com.caiye.binlogsql.handler;

import com.caiye.binlogsql.tool.SqlGenerateTool;
import com.caiye.binlogsql.tool.TableTool;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.caiye.binlogsql.filter.Filter;
import com.caiye.binlogsql.vo.RowVo;
import com.caiye.binlogsql.vo.TableVo;

import java.util.Collections;
import java.util.List;

import static com.caiye.binlogsql.tool.SqlGenerateTool.changeToRowVo;

@SuppressWarnings({"Duplicates", "AlibabaClassMustHaveAuthor"})
public class InsertHandler implements BinlogEventHandler {

    private final Filter filter;

    public InsertHandler(Filter filter) {
        this.filter = filter;
    }

    @Override
    public List<String> handle(Event event, boolean isTurn) {
        WriteRowsEventData writeRowsEventV2 = event.getData();

        TableVo tableVoInfo = TableTool.getTableInfo(writeRowsEventV2.getTableId());

        if (!filter.filter(tableVoInfo)) {
            return Collections.emptyList();
        }

        List<RowVo> rows = SqlGenerateTool.changeToRowVo(tableVoInfo, writeRowsEventV2.getRows());
        if (isTurn) {
            return SqlGenerateTool.deleteSql(tableVoInfo, rows, SqlGenerateTool.getComment(event.getHeader()));
        } else {
            return SqlGenerateTool.insertSql(tableVoInfo, rows, SqlGenerateTool.getComment(event.getHeader()));
        }
    }

}
