package com.gemantic.canal.listener;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.gemantic.canal.event.CanalEvent;
import com.gemantic.canal.event.InsertCanalEvent;
import com.gemantic.canal.event.UpdateCanalEvent;
import com.gemantic.canal.exception.ConvertException;
import com.gemantic.canal.msg.log.LogSender;
import com.gemantic.report.model.NReportDataDict;
import com.gemantic.report.repository.TreeReportServiceRepository;
import com.gemantic.springcloud.model.Response;
import com.gemantic.warehouse.model.DictColumn;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.gemantic.utils.LogMsgInitUtil.initNreportLog;

/**
 * RowData 字段信息，增量数据(修改后,新增后)
 * RowData 字段信息，增量数据(修改前,删除前) *
 */
@Slf4j
@Component
public class UpdateColumnListener extends CanalListener<UpdateCanalEvent> implements ApplicationContextAware {
    private ApplicationContext applicationContext;
    @Resource
    private TreeReportServiceRepository treeReportServiceRepository;
    @Value("${spring.application.name}")
    private String projectName;
    @Resource
    private LogSender logSender;


    @Override
    public void doSync(String database, String table, CanalEntry.RowData rowData, CanalEvent event) {
        if(!"dict_column".equals(table)) return;
        log.info("-------> database:{},table:{},eventType: UPDATE",database,table);
        List<CanalEntry.Column> columns = rowData.getAfterColumnsList();
        List<CanalEntry.Column> changeList = columns.stream().filter(obj -> obj.hasUpdated()).collect(Collectors.toList());
        DictColumn dictColumn = parseColumnsToObject(columns, DictColumn.class);
        //1.对应的nreport_data_dict是否存在
        ResponseEntity<Response<NReportDataDict>> responseEntity = treeReportServiceRepository.indicatorDetailByColumnId(dictColumn.getId());
        NReportDataDict oldReportDataDict = responseEntity.getBody().getData();
        if(oldReportDataDict == null){
            //insert
            applicationContext.publishEvent(event);
            return;
        }
        try {
            resetProperties(changeList, oldReportDataDict);
            treeReportServiceRepository.saveIndicator(Arrays.asList(oldReportDataDict));
        }catch (Exception e){
            log.info("处理数据错误-->{}", e);
            logSender.send(initNreportLog(event, e, projectName, this.getClass().getName(), "dict_column 更新错误"));

        }
    }

    private void resetProperties(List<CanalEntry.Column> changeColumns, NReportDataDict dataDict) throws ConvertException {
        try {
            for(CanalEntry.Column column : changeColumns){
                switch (column.getName()) {
                    case "table_name" : {
                        dataDict.setIndicatorTable("dw." + column.getValue());
                        break;
                    }
                    case "column_name" : {
                        dataDict.setIndicatorField(column.getValue());
                        break;
                    }
                    case "column_comment" : {
                        dataDict.setIndicatorName(column.getValue());
                        break;
                    }
                    case "column_unit" : {
                        dataDict.setUnit(column.getValue());
                        if (StringUtils.isEmpty(column.getValue())) {
                            dataDict.setIsUnitChange(null);
                            dataDict.setValueType(2);
                        }else {
                            dataDict.setIsUnitChange(0L);
                            dataDict.setValueType(1);
                        }
                        break;
                    }
                    case "is_entity" : {
                        String value = column.getValue();
                        if(StringUtils.isEmpty(value)){
                            dataDict.setIsEntity(null);
                        }else {
                            dataDict.setIsEntity(Integer.valueOf(value));
                        }
                        break;
                    }
                    case "dict_type" : {
                        dataDict.setEnumType(column.getValue());
                        break;
                    }
                    default: ;
                }
            }
        }catch (Exception e){
            throw new ConvertException("dict_column --> nreport_level_dict 转换错误", e);
        }

    }


    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
