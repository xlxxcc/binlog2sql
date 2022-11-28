package com.caiye.binlogsql.handler;

import com.caiye.binlogsql.tool.SqlGenerateTool;
import com.caiye.binlogsql.tool.TableTool;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.caiye.binlogsql.filter.Filter;
import com.caiye.binlogsql.vo.RowVo;
import com.caiye.binlogsql.vo.TableVo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.caiye.binlogsql.tool.SqlGenerateTool.changeToRowVo;

@SuppressWarnings({"Duplicates", "AlibabaClassMustHaveAuthor"})
public class UpdateHandler implements BinlogEventHandler {

    private final Filter filter;

    public UpdateHandler(Filter filterVo) {
        this.filter = filterVo;
    }

    @Override
    public List<String> handle(Event event, boolean isTurn) {
        UpdateRowsEventData updateRowsEventData = event.getData();
        TableVo tableVoInfo = TableTool.getTableInfo(updateRowsEventData.getTableId());

        if (!filter.filter(tableVoInfo)) {
            return Collections.emptyList();
        }
        List<Pair> updateRows = updateRowsEventData.getRows().stream().map(entry -> {
            RowVo key = SqlGenerateTool.changeToRowVo(tableVoInfo, entry.getKey());
            RowVo value = SqlGenerateTool.changeToRowVo(tableVoInfo, entry.getValue());
            return new Pair(key, value);
        }).collect(Collectors.toList());

        if (isTurn) {
            List<Pair> reversedPairs = updateRows.stream()
                    .map(rowPair -> new Pair(rowPair.getAfter(), rowPair.getBefore()))
                    .collect(Collectors.toList());
            return SqlGenerateTool.updateSql(tableVoInfo, reversedPairs, SqlGenerateTool.getComment(event.getHeader()));
        } else {
            return SqlGenerateTool.updateSql(tableVoInfo, updateRows, SqlGenerateTool.getComment(event.getHeader()));
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Pair {
        private RowVo before;
        private RowVo after;
    }

}
