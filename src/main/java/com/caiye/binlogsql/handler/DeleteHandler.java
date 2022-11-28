package com.caiye.binlogsql.handler;

import com.caiye.binlogsql.filter.Filter;
import com.caiye.binlogsql.tool.SqlGenerateTool;
import com.caiye.binlogsql.tool.TableTool;
import com.caiye.binlogsql.vo.RowVo;
import com.caiye.binlogsql.vo.TableVo;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;

import java.util.Collections;
import java.util.List;

@SuppressWarnings({"Duplicates", "AlibabaClassMustHaveAuthor"})
public class DeleteHandler implements BinlogEventHandler {

    private final Filter filter;

    public DeleteHandler(Filter filter) {
        this.filter = filter;
    }

    @Override
    public List<String> handle(Event event, boolean isTurn) {
        DeleteRowsEventData deleteRowsEventData = event.getData();
        TableVo tableVoInfo = TableTool.getTableInfo(deleteRowsEventData.getTableId());

        if (!filter.filter(tableVoInfo)) {
            return Collections.emptyList();
        }

        List<RowVo> rows = SqlGenerateTool.changeToRowVo(tableVoInfo, deleteRowsEventData.getRows());

        if (isTurn) {
            return SqlGenerateTool.insertSql(tableVoInfo, rows, SqlGenerateTool.getComment(event.getHeader()));
        } else {
            return SqlGenerateTool.deleteSql(tableVoInfo, rows, SqlGenerateTool.getComment(event.getHeader()));
        }
    }

}
