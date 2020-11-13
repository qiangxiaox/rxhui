package com.gemantic.canal.listener;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.gemantic.canal.event.CanalEvent;
import com.gemantic.canal.event.InsertCanalEvent;
import com.gemantic.canal.msg.log.LogSender;
import com.gemantic.canal.service.InsertTableService;
import com.gemantic.warehouse.model.DictTable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

import static com.gemantic.utils.LogMsgInitUtil.initNreportLog;

/**
 * RowData 字段信息，增量数据(修改后,新增后)
 */
@Slf4j
@Component
public class InsertTableListener extends CanalListener<InsertCanalEvent> {
    @Resource
    private InsertTableService insertTableService;
    @Resource
    private LogSender logSender;
    @Value("${spring.application.name}")
    private String projectName;


    @Override
    public void doSync(String database, String table, CanalEntry.RowData rowData, CanalEvent event) {
        if(!"dict_table".equals(table)) return;
        log.info("-------> database:{},table:{},eventType: INSERT",database,table);
        List<CanalEntry.Column> columns = rowData.getAfterColumnsList();
        DictTable dictTable = parseColumnsToObject(columns, DictTable.class);
        try {
            insertTableService.saveTable(dictTable);
        } catch (Exception e) {
            log.info("处理数据错误-->{}", e);
            logSender.send(initNreportLog(event, e, projectName, this.getClass().getName(), "dict_table 新增错误"));
        }
    }
}
