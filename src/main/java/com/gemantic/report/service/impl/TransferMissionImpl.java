package com.gemantic.report.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.gemantic.dict.model.Dict;
import com.gemantic.report.constant.DictPrefixConstant;
import com.gemantic.report.constant.EntityBasicMess;
import com.gemantic.report.model.*;
import com.gemantic.report.repository.KnowledgeDomainReporistory;
import com.gemantic.report.repository.TreeReportServiceRepository;
import com.gemantic.report.service.TransferMission;
import com.gemantic.springcloud.model.Response;
import com.gemantic.springcloud.utils.MsgUtil;
import com.gemantic.warehouse.model.DictTable;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * createed By xiaoqiang
 * 2019/12/9 14:28
 */
@Slf4j
@Service("transferToReport")
public class TransferMissionImpl implements TransferMission {

	@Resource
	DictPrefixConstant dictPrefixConstant;

	@Resource
	TreeReportServiceRepository treeRepository;
	@Resource
	KnowledgeDomainReporistory knowledgeDomainReporistory;
	@Resource
	EntityBasicMess entityBasicMess;

	private ThemeTransfer transferResult;

	private List<NReportLevelTree> treeStruct;

	@Override
	public void transer(TransferDataSnapshoot dataSnapshoot){
		//读取原有树节点
		ResponseEntity<Response<List<NReportLevelTree>>> treeStructRes = treeRepository.getNewTreeList();
		if(!MsgUtil.isValidResponseMessage(treeStructRes)){
			log.error("树节点获取错误");
		}
		treeStruct = treeStructRes.getBody().getData();

		//1.拆分表信息，组装主题信息-->保存主题信息并生成快照
		this.transferResult = loadTheme(dataSnapshoot.getDictTable());

		//2.根据表信息和字段信息，生成报告中的指标信息 --> 保存指标信息并生成快照
		List<NReportDataDict> reportDataDicts = loadIndicators(dataSnapshoot);
		if(CollectionUtils.isEmpty(reportDataDicts)) return;

		//3.保存词典信息
//		List<Dict> propDict = knowledgeDomainReporistory.findPropDict();
//		DictTransfer dictTransfer = loadDict(reportDataDicts, propDict);

		//4.刷新词典
		treeRepository.refresh();
	}

	/**
	 * 新增词典信息
	 * @param reportDataDicts
	 * @param propDict
	 */
	private DictTransfer loadDict(List<NReportDataDict> reportDataDicts, List<Dict> propDict){
		DictTransfer dictTransfer = new DictTransfer(reportDataDicts, entityBasicMess.getBasicMessMap(), propDict);
		knowledgeDomainReporistory.saveDict(dictTransfer.getResultDict());
		return dictTransfer;
	}

	/**
	 * 新增指标信息
	 * @param dataSnapshoot
	 * @return
	 */
	private List<NReportDataDict> loadIndicators(TransferDataSnapshoot dataSnapshoot) {
		IndicatorTransfer indicatorTransfer = new IndicatorTransfer(dataSnapshoot, transferResult, dictPrefixConstant.getEntityMap());
		List<NReportDataDict> reportDataDicts = indicatorTransfer.getReportDataDicts();
		Function<Map.Entry<String, List<NReportDataDict>>, Stream<NReportDataDict>> flatFunction = obj -> obj.getValue().stream();
		List<NReportDataDict> repeatData = reportDataDicts.stream().collect(Collectors.groupingBy(obj -> obj.getIndicatorName() + obj.getLevelCode()))
				.entrySet().stream().filter(obj -> obj.getValue().size() > 1)
				.flatMap(flatFunction).collect(Collectors.toList());
		if(repeatData.size() > 0) log.error("重复指标json={}", JSONObject.toJSONString(repeatData));
		reportDataDicts.removeAll(repeatData);
		ResponseEntity<Response<List<NReportDataDict>>> dictRes = treeRepository.saveIndicator(reportDataDicts);
		List<NReportDataDict> nReportDataDicts = dictRes.getBody().getData();
		log.info("新增字典成功");
		return nReportDataDicts;
	}

	/**
	 * 新增树节点信息，并返回最底层的树信息
	 * @param dictTable
	 * @return
	 */
	private ThemeTransfer loadTheme(List<DictTable> dictTable){
		Map<String, String> prefixMap = dictPrefixConstant.getDictPrefixMap();
		ThemeTransfer themeTransfer = new ThemeTransfer(dictTable, prefixMap, treeStruct);
		List<NReportLevelTree> topNodeInfos = themeTransfer.getTopNodeInfos();
		List<NReportLevelTree> middleNodeInfos = themeTransfer.getMiddleNodeInfos();
		List<NReportLevelTree> tableNodeInfos = themeTransfer.getTabNodeInfos();

		//先保存顶级结点
		List<NReportLevelTree> topInsertResult = Lists.newArrayList();
		if(topNodeInfos.size() > 0){
			ResponseEntity<Response<List<NReportLevelTree>>> topRes = treeRepository.saveTree(topNodeInfos);
			List<NReportLevelTree> topData = topRes.getBody().getData();
			if(CollectionUtils.isNotEmpty(topData)){
				topInsertResult.addAll(topData);
			}
			log.info("顶级节点新增成功");
		}
		topInsertResult.addAll(themeTransfer.getTopVo());
		themeTransfer.setTopNodeInfos(topInsertResult);

		//处理中间节点
		List<NReportLevelTree> middleResult = Lists.newArrayList();
		if(middleNodeInfos.size() > 0){
			List<NReportLevelTree> middleData = saveMiddleTheme(middleNodeInfos, themeTransfer.getTopNodeInfos());
			if(CollectionUtils.isNotEmpty(middleData)){
				middleResult.addAll(middleData);
			}
		}
		middleResult.addAll(themeTransfer.getMiddleVo());
		themeTransfer.setMiddleNodeInfos(middleResult);

		//处理表级别的数据
		List<NReportLevelTree> tabResult = Lists.newArrayList();
		if(tableNodeInfos.size() > 0){
			List<NReportLevelTree> tabData = saveTableTheme(themeTransfer.getMiddleNodeInfos(), themeTransfer.getTopNodeInfos(), tableNodeInfos);
			if(CollectionUtils.isNotEmpty(tabData)){
				tabResult.addAll(tabData);
			}
		}
		tabResult.addAll(themeTransfer.getTabVo());
		themeTransfer.setTabNodeInfos(tabResult);
		return themeTransfer;
	}

	private List<NReportLevelTree> saveMiddleTheme(List<NReportLevelTree> middleNodeInfos,
	                                            List<NReportLevelTree> topNodeInfos){
		//保存中间节点
		for(NReportLevelTree topNode : topNodeInfos){
			Long topId = topNode.getId();
			String topPrefix = topNode.getAliasName();
			for(NReportLevelTree tree : middleNodeInfos){
				if(tree.getAliasName().startsWith(topPrefix)){
					tree.setParentId(topId);
					tree.setParentPath(topId.toString());
				}
			}
		}
		ResponseEntity<Response<List<NReportLevelTree>>> middleRes = treeRepository.saveTree(middleNodeInfos);
		List<NReportLevelTree> middleResult = middleRes.getBody().getData();
		log.info("中间节点新增成功");
		return middleResult;
	}

	private List<NReportLevelTree> saveTableTheme(List<NReportLevelTree> middleNodeInfos,
	                                               List<NReportLevelTree> topInsertResult,
	                                              List<NReportLevelTree> tableNodeInfos){
		//保存底层节点
		for(NReportLevelTree levelTree : topInsertResult){
			Long topId = levelTree.getId();
			List<NReportLevelTree> midNode = middleNodeInfos.stream().filter(obj -> obj.getParentId().equals(topId)).collect(Collectors.toList());
			for(NReportLevelTree mid : midNode){
				String midPrefix = mid.getAliasName();
				Long midMessId = mid.getId();
				mid.setId(midMessId);
				for(NReportLevelTree tableTree : tableNodeInfos){
					if(tableTree.getAliasName().startsWith(midPrefix)){
						tableTree.setParentId(midMessId);
						tableTree.setParentPath(midMessId+ "," + topId.toString());
					}
				}
			}
		}
		ResponseEntity<Response<List<NReportLevelTree>>> tabRes = treeRepository.saveTree(tableNodeInfos);
		List<NReportLevelTree> tabResult = tabRes.getBody().getData();
		log.info("底层节点新增成功");
		return tabResult;
	}

}
