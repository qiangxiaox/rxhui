package com.gemantic.canal.service;

import com.gemantic.canal.exception.ConvertException;
import com.gemantic.canal.exception.HttpErrorException;
import com.gemantic.canal.exception.ParentNullException;
import com.gemantic.canal.builder.IndicatorBuilder;
import com.gemantic.report.constant.DictPrefixConstant;
import com.gemantic.report.model.NReportDataDict;
import com.gemantic.report.model.NReportLevelTree;
import com.gemantic.report.repository.MetadataThemeRepository;
import com.gemantic.report.repository.TreeReportServiceRepository;
import com.gemantic.springcloud.model.Response;
import com.gemantic.warehouse.model.DictColumn;
import com.gemantic.warehouse.model.DictTable;
import com.gemantic.warehouse.model.SchemaColumns;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;

/**
 * createed By xiaoqiang
 * 2020/11/9 14:46
 */
@Service
public class InsertColumnService {

	@Resource
	private TreeReportServiceRepository treeReportServiceRepository;
	@Resource
	private DictPrefixConstant dictPrefixConstant;
	@Resource
	private MetadataThemeRepository metadataThemeRepository;

	public void saveColumn(DictColumn dictColumn) throws ParentNullException, HttpErrorException, ConvertException {
		NReportDataDict reportDataDict = transIndicator(dictColumn);
		try {
			treeReportServiceRepository.saveIndicator(Arrays.asList(reportDataDict));
		}catch (Exception e){
			throw new HttpErrorException("指标节点保存异常", e);
		}
	}


	private NReportDataDict transIndicator(DictColumn dictColumn) throws ParentNullException, ConvertException {
		IndicatorBuilder builder = new IndicatorBuilder();
		builder.dictColumn(dictColumn);
		builder.entityMap(dictPrefixConstant.getEntityMap());
		//获取dict_column对应的DictTable
		ResponseEntity<Response<DictTable>> tableRes = metadataThemeRepository.selectTableByTableName(dictColumn.getTableName());
		DictTable dictTable = tableRes.getBody().getData();

		//获取dict_column对应的nReportLevelTree
		ResponseEntity<Response<List<NReportLevelTree>>> treeRes = treeReportServiceRepository.findTreeByAliasName(Arrays.asList(dictColumn.getTableName()));
		List<NReportLevelTree> levelTrees = treeRes.getBody().getData();

		//获取dict_column对应的SchemaColumns
		ResponseEntity<Response<SchemaColumns>> schemaColumnRes = metadataThemeRepository.selectSchemaColumn(dictColumn.getOwnerName(), dictColumn.getTableName(), dictColumn.getColumnName());
		SchemaColumns schemaColumns = schemaColumnRes.getBody().getData();

		if (dictTable != null) {
			builder.tableMess(dictTable);
		} else {
			throw new ParentNullException("dict_table表中数据不存在",  ParentNullException.MsgType.dict_table);
		}

		if (CollectionUtils.isNotEmpty(levelTrees)) {
			builder.nReportLevelTree(levelTrees.get(0));
		} else {
			throw new ParentNullException("nreport_level_tree表中数据不存在", ParentNullException.MsgType.nreport_level_tree);
		}

		if (schemaColumns != null) {
			builder.schemaColumns(schemaColumns);
		}
		return builder.build();
	}
}
