package com.gemantic.canal.listener;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.gemantic.canal.event.CanalEvent;
import com.gemantic.canal.event.InsertCanalEvent;
import com.gemantic.canal.exception.ParentNullException;
import com.gemantic.canal.msg.delay.DelaySender;
import com.gemantic.canal.msg.log.LogSender;
import com.gemantic.canal.service.InsertColumnService;
import com.gemantic.warehouse.model.DictColumn;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
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
public class InsertColumnListener extends CanalListener<InsertCanalEvent> {
    @Resource
    private InsertColumnService insertColumnService;
    @Resource
    private DelaySender delaySender;
    @Resource
    private LogSender logSender;
    @Value("${spring.application.name}")
    private String projectName;

    @Override
    public void doSync(String database, String table, CanalEntry.RowData rowData, CanalEvent event) {
        if(!"dict_column".equals(table)) return;
        log.info("-------> database:{},table:{},eventType: INSERT",database,table);
        List<CanalEntry.Column> columns = rowData.getAfterColumnsList();
        DictColumn dictColumn = parseColumnsToObject(columns, DictColumn.class);

        try {
            insertColumnService.saveColumn(dictColumn);
        } catch (ParentNullException e) {
            log.info("获取数据错误 --> {}", e);
            //重试3次
            if(event.getCount() <= 3){
                //发送到延迟队列中
                delaySender.send(event);
            }else {
                //超过3次保存到表中
                logSender.send(initNreportLog(event, e, projectName, this.getClass().getName(), "dict_column 新增错误"));
            }
        } catch (Exception e){
            log.info("处理数据错误 -->{}", e);
            logSender.send(initNreportLog(event, e, projectName, this.getClass().getName(), "dict_column 新增错误"));
        }
    }


}
