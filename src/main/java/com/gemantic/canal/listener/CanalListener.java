package com.gemantic.canal.listener;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.gemantic.canal.event.CanalEvent;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 自定义事件监听器
 *
 */
@Slf4j
public abstract class CanalListener<E extends CanalEvent> implements ApplicationListener<E> {

    /**
     * 重写处理事件方法
     * 共性处理操作
     *
     * @param event 事件对象
     */
    @Override
    public void onApplicationEvent(E event) {
        CanalEntry.Entry entry = event.getEntry();
        String database = entry.getHeader().getSchemaName();
        String table = entry.getHeader().getTableName();
        CanalEntry.RowChange change;
        try {
            change = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        } catch (InvalidProtocolBufferException e) {
            log.error("根据CanalEntry获取RowChange失败！", e);
            return;
        }
        change.getRowDatasList().forEach(rowData -> doSync(database, table, rowData, event));
    }

    /**
     * 实现者更具不同的操作来实现此方法
     *  @param database 数据库名
     * @param table 表明
     * @param rowData 数据
     * @param entry
     */
    public abstract void doSync(String database, String table, CanalEntry.RowData rowData, CanalEvent entry);

    /**
     * columns转json
     *
     * @param columns
     * @return
     */
    public Map<String, Object> parseColumnsToMap(List<CanalEntry.Column> columns) {
        Map<String, Object> jsonMap = new HashMap<>();
        columns.forEach(column -> {
            if (column == null) {
                return;
            }
            jsonMap.put(column.getName(), column.getValue());
        });
        return jsonMap;
    }

    /**
     * columns转<T>对象
     *
     * @param columns
     * @return
     */
    public <T> T parseColumnsToObject(List<CanalEntry.Column> columns, Class<T> clazz) {
        return JSON.parseObject(JSON.toJSONString(parseColumnsToMap(columns)), clazz);
    }
}
