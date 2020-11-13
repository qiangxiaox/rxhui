package com.gemantic.report.model;

import com.gemantic.report.constant.DataSource;
import com.gemantic.report.constant.EntityBasicMess;
import com.gemantic.warehouse.model.DictTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.gemantic.report.constant.DataSource.filter_prefix;
import static com.gemantic.report.constant.DataSource.split_placeholder;

/**
 * createed By xiaoqiang
 * 2019/12/7 16:19
 */
public class ThemeTransfer {
	//手动维护的字典表
	private List<DictTable> dictTable;
	private static Integer default_sort = 100;
	private static Integer basic_sort = 1;

	//需要保存到数据库的信息
	@Getter
	@Setter
	private List<NReportLevelTree> topNodeInfos = Lists.newArrayList();
	@Getter
	@Setter
	private List<NReportLevelTree> middleNodeInfos = Lists.newArrayList();
	@Getter
	@Setter
	private List<NReportLevelTree> tabNodeInfos = Lists.newArrayList();
	@Getter
	private Map<String, NReportLevelTree> reportThemeMap = Maps.newHashMap();

	//获取已有的结构，去重
	@Getter
	private List<NReportLevelTree> topVo = Lists.newArrayList();
	@Getter
	private List<NReportLevelTree> middleVo = Lists.newArrayList();
	@Getter
	private List<NReportLevelTree> tabVo = Lists.newArrayList();

	public ThemeTransfer(List<DictTable> dictTable, Map<String, String> dictPrefixMap, List<NReportLevelTree> treeStruct) {
		this.dictTable = dictTable;
		initTreeVo(treeStruct);
		initLevelTree(dictPrefixMap);
	}

	private void initTreeVo(List<NReportLevelTree> treeStruct) {
		if(CollectionUtils.isEmpty(treeStruct)){
			return;
		}
		Map<String, NReportLevelTree> levelTreeMap = treeStruct.stream()
				.collect(Collectors.toMap(NReportLevelTree::getAliasName, Function.identity()));
		reportThemeMap.putAll(levelTreeMap);
		List<NReportLevelTree> topNode = treeStruct.stream()
				.filter(obj -> obj.getParentId() == null)
				.collect(Collectors.toList());
		topVo.addAll(topNode);
		List<NReportLevelTree> middleTree = treeStruct.stream()
				.filter(obj -> obj.getParentId() != null)
				.filter(obj -> topNode.stream().anyMatch(pid -> obj.getParentId().equals(pid.getId())))
				.collect(Collectors.toList());
		middleVo.addAll(middleTree);
		List<NReportLevelTree> tabTree = treeStruct.stream()
				.filter(obj -> obj.getParentId() != null)
				.filter(obj -> middleTree.stream().anyMatch(pid -> obj.getParentId().equals(pid.getId())))
				.collect(Collectors.toList());
		tabVo.addAll(tabTree);
	}


	private void initLevelTree(Map<String, String> dictPrefixMap){
		Long timeMillis = System.currentTimeMillis();
		for(DictTable table : dictTable) {
			String tableName = table.getTableName();
			String[] themes = tableName.split(split_placeholder);
			if(filter_prefix.equals(themes[0])) continue;
			String middle = themes[0] + split_placeholder + themes[1];

			if(reportThemeMap.get(themes[0]) == null){
				NReportLevelTree topLevelTree = initLevelTree(dictPrefixMap.get(themes[0]), timeMillis, themes[0], default_sort);
				reportThemeMap.put(themes[0], topLevelTree);
				if(!topVo.contains(topLevelTree)){
					topNodeInfos.add(topLevelTree);
				}
			}

			if(reportThemeMap.get(middle) == null){
				NReportLevelTree midLevelTree = initLevelTree(dictPrefixMap.get(themes[1]), timeMillis, middle, default_sort);
				reportThemeMap.put(middle, midLevelTree);
				if(!middleVo.contains(midLevelTree)){
					middleNodeInfos.add(midLevelTree);
				}
			}

			Integer sortValue = default_sort;
			if(EntityBasicMess.basicMessList.contains(tableName)){
				sortValue = basic_sort;
			}
			NReportLevelTree tabLevelTree = initLevelTree(table.getTableComment(), timeMillis, tableName, sortValue);
			reportThemeMap.put(tableName, tabLevelTree);
			//根据aliasName和 表备注判断是否已经存在该节点
			if(tabVo.stream().noneMatch(obj -> obj.getAliasName().equals(tabLevelTree.getAliasName())
					&& obj.getName().equals(tabLevelTree.getName()))){
				tabNodeInfos.add(tabLevelTree);
			}
		}
	}

	private NReportLevelTree initLevelTree(String treeName, Long timeMillis, String aliasName, Integer sortValue){
		return NReportLevelTree.builder().name(treeName).sourceName(DataSource.DATA_WAREHOUSE).sortValue(sortValue)
				.createAt(timeMillis).updateAt(timeMillis).aliasName(aliasName).isValid(1).build();
	}
}
