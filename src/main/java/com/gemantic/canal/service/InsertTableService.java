package com.gemantic.canal.service;

import com.gemantic.canal.exception.ConvertException;
import com.gemantic.canal.exception.HttpErrorException;
import com.gemantic.canal.builder.TreeNodeBuilder;
import com.gemantic.report.constant.DictPrefixConstant;
import com.gemantic.report.model.NReportLevelTree;
import com.gemantic.report.repository.TreeReportServiceRepository;
import com.gemantic.springcloud.model.Response;
import com.gemantic.warehouse.model.DictTable;
import org.apache.commons.collections4.MapUtils;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * createed By xiaoqiang
 * 2020/11/11 11:02
 */
@Service
public class InsertTableService {

	@Resource
	TreeReportServiceRepository treeReportServiceRepository;
	@Resource
	DictPrefixConstant dictPrefixConstant;

	public void saveTable(DictTable dictTable) throws ConvertException, HttpErrorException {
		TreeNodeBuilder treeNodeBuilder = new TreeNodeBuilder();
		treeNodeBuilder.dictPrefixMap(dictPrefixConstant.getDictPrefixMap());
		treeNodeBuilder.dictTable(dictTable);
		treeNodeBuilder.treeReportServiceRepository(treeReportServiceRepository);
		Map<String, NReportLevelTree> treeMap = treeNodeBuilder.build();
		if(MapUtils.isEmpty(treeMap)) return;
		saveTable(treeMap);

	}

	private void saveTable(Map<String, NReportLevelTree> treeMap) throws HttpErrorException {
		try {
			NReportLevelTree topTree = treeMap.get("top");
			NReportLevelTree midTree = treeMap.get("middle");
			NReportLevelTree tabTree = treeMap.get("tab");

			if(topTree != null){
				//保存顶级节点
				ResponseEntity<Response<List<NReportLevelTree>>> topTreeRes = treeReportServiceRepository.saveTree(Arrays.asList(topTree));
				NReportLevelTree topTreeResData = topTreeRes.getBody().getData().get(0);
				midTree.setParentId(topTreeResData.getId());
				midTree.setParentPath(topTreeResData.getId().toString());
				treeMap.remove("top");
			}

			if(midTree != null){
				//保存主题节点
				ResponseEntity<Response<List<NReportLevelTree>>> midTreeRes = treeReportServiceRepository.saveTree(Arrays.asList(midTree));
				NReportLevelTree midTreeResData = midTreeRes.getBody().getData().get(0);
				tabTree.setParentId(midTreeResData.getId());
				tabTree.setParentPath(midTreeResData.getParentPath()  + "," + midTreeResData.getId().toString());
				treeMap.remove("middle");
			}
			//保存表级节点
			treeReportServiceRepository.saveTree(Arrays.asList(tabTree));
			treeMap.remove("tab");
		}catch (Exception e){
			throw new HttpErrorException("树节点保存异常");
		}

	}

}
