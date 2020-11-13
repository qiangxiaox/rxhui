package com.gemantic.canal.listener;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.gemantic.canal.event.CanalEvent;
import com.gemantic.canal.event.DeleteCanalEvent;
import com.gemantic.canal.msg.log.LogSender;
import com.gemantic.report.repository.TreeReportServiceRepository;
import com.gemantic.warehouse.vo.DictTable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import static com.gemantic.utils.LogMsgInitUtil.initNreportLog;

/**
 *RowData 字段信息，增量数据(修改前,删除前) *
 */
@Slf4j
@Component
public class DeleteTableListener extends CanalListener<DeleteCanalEvent> {

    @Resource
    TreeReportServiceRepository treeReportServiceRepository;
    @Resource
    private LogSender logSender;
    @Value("${spring.application.name}")
    private String projectName;

    @Override
    public void doSync(String database, String table, CanalEntry.RowData rowData, CanalEvent event) {
        if(!"dict_table".equals(table)) return;
        log.info("-------> database:{},table:{},eventType: DELETE",database,table);
        DictTable dictTable = parseColumnsToObject(rowData.getBeforeColumnsList(), DictTable.class);
        try {
            treeReportServiceRepository.deleteLevelTreeByDictTableId(dictTable.getId());
        }catch (Exception e){
            log.info("处理数据错误 -->{}", e);
            logSender.send(initNreportLog(event, e, projectName, this.getClass().getName(), "dict_table 删除错误"));
        }
    }
}
