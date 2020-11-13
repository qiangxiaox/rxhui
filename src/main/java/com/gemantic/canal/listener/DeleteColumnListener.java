package com.gemantic.canal.listener;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.gemantic.canal.event.CanalEvent;
import com.gemantic.canal.event.DeleteCanalEvent;
import com.gemantic.canal.msg.log.LogSender;
import com.gemantic.report.repository.TreeReportServiceRepository;
import com.gemantic.warehouse.vo.DictColumn;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

import static com.gemantic.utils.LogMsgInitUtil.initNreportLog;

/**
 *RowData 字段信息，增量数据(修改前,删除前) *
 */
@Slf4j
@Component
public class DeleteColumnListener extends CanalListener<DeleteCanalEvent> {

    @Resource
    TreeReportServiceRepository treeReportServiceRepository;
    @Resource
    private LogSender logSender;
    @Value("${spring.application.name}")
    private String projectName;

    @Override
    public void doSync(String database, String table, CanalEntry.RowData rowData, CanalEvent event) {
        if(!"dict_column".equals(table)) return;
        log.info("-------> database:{},table:{},eventType: DELETE",database,table);
        List<CanalEntry.Column> columns = rowData.getBeforeColumnsList();
        DictColumn dictColumn = parseColumnsToObject(columns, DictColumn.class);
        try{
            treeReportServiceRepository.deleteIndicatorByColumnId(dictColumn.getId());
        }catch (Exception e){
            log.info("处理数据错误 -->{}", e);
            logSender.send(initNreportLog(event, e, projectName, this.getClass().getName(), "dict_column 删除错误"));
        }
    }

}
