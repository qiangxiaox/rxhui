package com.gemantic.canal.listener;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.gemantic.canal.builder.IndicatorBuilder;
import com.gemantic.canal.event.CanalEvent;
import com.gemantic.canal.event.InsertCanalEvent;
import com.gemantic.canal.event.UpdateCanalEvent;
import com.gemantic.canal.msg.log.LogSender;
import com.gemantic.report.model.NReportDataDict;
import com.gemantic.report.model.NReportLevelTree;
import com.gemantic.report.repository.TreeReportServiceRepository;
import com.gemantic.springcloud.model.Response;
import com.gemantic.warehouse.model.DictTable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
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
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.gemantic.utils.LogMsgInitUtil.initNreportLog;

/**
 * RowData 字段信息，增量数据(修改后,新增后)
 * RowData 字段信息，增量数据(修改前,删除前)
 */
@Slf4j
@Component
public class UpdateTableListener extends CanalListener<UpdateCanalEvent> implements ApplicationContextAware {
    private ApplicationContext applicationContext;


    private static final String pk_column = "pk_column";
    private static final String table_name = "table_name";

    @Resource
    private TreeReportServiceRepository treeReportServiceRepository;

    @Value("${spring.application.name}")
    private String projectName;
    @Resource
    private LogSender logSender;

    @Override
    public void doSync(String database, String table, CanalEntry.RowData rowData, CanalEvent event) {
        if(!"dict_table".equals(table)) return;
        log.info("-------> database:{},table:{},eventType: UPDATE",database,table);
        List<CanalEntry.Column> beforeColumns = rowData.getBeforeColumnsList();
        List<CanalEntry.Column> columns = rowData.getAfterColumnsList();
        Map<String, CanalEntry.Column> changeMap = columns.stream().filter(obj -> obj.hasUpdated()).collect(Collectors.toMap(obj -> obj.getName(), Function.identity(), (a, b) -> a));
        DictTable dictTable = parseColumnsToObject(columns, DictTable.class);
        try {
            //1.对应的nreport_level_tree是否存在
            ResponseEntity<Response<NReportLevelTree>> responseEntity = treeReportServiceRepository.findTreeByTableId(dictTable.getId());
            NReportLevelTree reportLevelTree = responseEntity.getBody().getData();
            if (reportLevelTree == null) {
                //2.insert
                applicationContext.publishEvent(event);
                return;
            }

            //pk_column change
            boolean pkChanged = changeMap.containsKey(pk_column);
            if (pkChanged) {
                pkChangedProcess(dictTable, reportLevelTree);
            }

            //table_name change
            boolean nameChanged = changeMap.containsKey(table_name);
            if (nameChanged) {
                nameChangedProcess(dictTable, reportLevelTree);
            }
        }catch (Exception e){
            log.info("处理数据错误-->{}", e);
            logSender.send(initNreportLog(event, e, projectName, this.getClass().getName(), "dict_table 更新错误"));
        }
    }


    private void nameChangedProcess(DictTable dictTable, NReportLevelTree reportLevelTree){
        String tableName = dictTable.getTableName();
        reportLevelTree.setAliasName(tableName);
        reportLevelTree.setName(dictTable.getTableComment());
        treeReportServiceRepository.saveTree(Arrays.asList(reportLevelTree));
    }

    private void pkChangedProcess(DictTable dictTable, NReportLevelTree reportLevelTree){
        ResponseEntity<Response<List<NReportDataDict>>> indicatorRes = treeReportServiceRepository.indicatorDetailByLevelCode(reportLevelTree.getId());
        List<NReportDataDict> reportDataDicts = indicatorRes.getBody().getData();
        if(CollectionUtils.isEmpty(reportDataDicts)) return;

        String newDynamic = IndicatorBuilder.getDynamic(dictTable.getPkColumn());
        NReportDataDict dataDict = reportDataDicts.stream().filter(obj -> StringUtils.isNotEmpty(obj.getDynamicField())).findFirst().get();
        String oldDynamicField = dataDict.getDynamicField();
        if(newDynamic.equals(oldDynamicField)) return;
        for (NReportDataDict reportDataDict : reportDataDicts) {
            reportDataDict.setDynamicField(newDynamic);
            transNewIndicator(reportDataDict, newDynamic);
        }
        treeReportServiceRepository.saveIndicator(reportDataDicts);
    }

    private void transNewIndicator(NReportDataDict reportDataDict, String newDynamic){
        //确定对比类型
        String compare = "1";
        //确定查询时间字段
        String dateField = null;
        String[] dynamicField = newDynamic.split(",");
        if (StringUtils.isEmpty(dynamicField[0])) {
            compare = "5"; //分类对比
        } else {
            dateField = dynamicField[0];
        }
        String field = reportDataDict.getIndicatorField();
        Long defaultCompare = null;
        String defautDateFre = "日,周,月,季,年,年（3月）,年（6月）,年（9月）";
        String defautImage = "1";
        Long indexFlag = null;
        if (field.equals(dynamicField[1])) {
            indexFlag = 1L; //字段标记
        }
        if (field.equals(dateField)) {
            defaultCompare = 1L; //默认对比属性设置
            indexFlag = 3L; //字段标记
            defautDateFre  = null;
            newDynamic = null;
            compare = null;
            defautImage = null;
        }
        reportDataDict.setCompareCode(compare);
        reportDataDict.setDefaultCompareCode(defaultCompare);
        reportDataDict.setImageCode(defautImage);
        reportDataDict.setDataRate(defautDateFre);
        reportDataDict.setIndexFlag(indexFlag);
        reportDataDict.setDynamicField(newDynamic);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
