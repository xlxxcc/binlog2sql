package com.caiye.binlogsql;

import com.caiye.binlogsql.handler.BinlogEventHandler;
import com.github.shyiko.mysql.binlog.event.Event;
import lombok.AllArgsConstructor;

import java.util.List;

@SuppressWarnings({"AlibabaClassMustHaveAuthor"})
@AllArgsConstructor
public class ProxyBinlogEventHandler implements BinlogEventHandler {

    private BinlogEventHandler binlogEventHandler;
    private List<SqlTestVo> sqlTestVos;

    @Override
    public List<String> handle(Event event, boolean isTurn) {
        List<String> sql = binlogEventHandler.handle(event, false);
        List<String> rollSql = binlogEventHandler.handle(event, true);
        if (!sql.isEmpty()) {
            sqlTestVos.add(new SqlTestVo(sql, rollSql));
        }
        return sql;
    }
}
