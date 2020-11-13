package com.gemantic.canal.builder;

import com.gemantic.canal.exception.ConvertException;
import com.gemantic.report.constant.DataSource;
import com.gemantic.report.model.NReportLevelTree;
import com.gemantic.report.repository.TreeReportServiceRepository;
import com.gemantic.springcloud.model.Response;
import com.gemantic.warehouse.model.DictTable;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.http.ResponseEntity;

import java.util.*;

import static com.gemantic.report.constant.DataSource.filter_prefix;
import static com.gemantic.report.constant.DataSource.split_placeholder;

/**
 * createed By xiaoqiang
 * 2020/10/20 17:25
 */
public class TreeNodeBuilder {

	/*对应的dictTable信息*/
	private DictTable dictTable;
	/*主题信息*/
	private Map<String, String> dictPrefixMap;
	private TreeReportServiceRepository treeReportServiceRepository;

	public TreeNodeBuilder() { }

	public TreeNodeBuilder dictTable(DictTable dictTable){
		this.dictTable = dictTable;
		return this;
	}

	public TreeNodeBuilder dictPrefixMap(Map<String, String> dictPrefixMap){
		this.dictPrefixMap = dictPrefixMap;
		return this;
	}

	public TreeNodeBuilder treeReportServiceRepository(TreeReportServiceRepository treeReportServiceRepository){
		this.treeReportServiceRepository = treeReportServiceRepository;
		return this;
	}

	/**
	 *
	 * @return 返回值结构如下
	 * （key = top）    --> 顶级结点
	 * （key = middle） --> 主题节点
	 * （key = tab）    --> 表节点
	 */
	public Map<String, NReportLevelTree> build() throws ConvertException {
		try {
			Long timeMillis = System.currentTimeMillis();
			String tableName = dictTable.getTableName();
			String[] themes = tableName.split(split_placeholder);

			if (filter_prefix.equals(themes[0])) return Collections.emptyMap();

			String middle = themes[0] + split_placeholder + themes[1];

			//根据aliasName获取上述节点信息
			ResponseEntity<Response<List<NReportLevelTree>>> treeListRes = treeReportServiceRepository.findTreeByAliasName(Arrays.asList(themes[0], middle, tableName));
			List<NReportLevelTree> treeList = treeListRes.getBody().getData();

			Map<String, NReportLevelTree> result = Maps.newHashMap();
			//都不存在，全部拆分
			if (CollectionUtils.isEmpty(treeList)) {
				//顶级节点
				NReportLevelTree topTree = initLevelTree(dictPrefixMap.get(themes[0]), timeMillis, null, null, themes[0], null);
				result.put("top", topTree);

				//主题节点
				NReportLevelTree middleTree = initLevelTree(dictPrefixMap.get(themes[1]), timeMillis, null, null, middle, null);
				result.put("middle", middleTree);

				//表节点
				NReportLevelTree tabTree = initLevelTree(dictTable.getTableComment(), timeMillis, null, null, tableName, dictTable.getId());
				result.put("tab", tabTree);
				return result;
			}

			//表级节点存在，更新nreport_level_tree对应的dict_table_id
			Optional<NReportLevelTree> tabTree = treeList.stream().filter(obj -> tableName.equalsIgnoreCase(obj.getAliasName())).findFirst();
			if (tabTree.isPresent()) {
				NReportLevelTree nReportLevelTree = tabTree.get();
				nReportLevelTree.setDictTableId(dictTable.getId());
				result.put("tab", nReportLevelTree);
				return result;
			}

			//不为空，意味着至少会存在一个顶级节点
			NReportLevelTree topTree = treeList.stream().filter(obj -> themes[0].equalsIgnoreCase(obj.getAliasName())).findFirst().get();

			Optional<NReportLevelTree> middleTreeOp = treeList.stream().filter(obj -> middle.equalsIgnoreCase(obj.getAliasName())).findFirst();
			if (!middleTreeOp.isPresent()) {
				NReportLevelTree middleTree = initLevelTree(dictPrefixMap.get(themes[1]), timeMillis, topTree.getId(), topTree.getId().toString(), middle, null);
				result.put("middle", middleTree);
			}
			NReportLevelTree tabTreeInit = initLevelTree(dictTable.getTableComment(), timeMillis,
					middleTreeOp.isPresent() ? middleTreeOp.get().getId() : null,
					middleTreeOp.isPresent() ? topTree.getId().toString() + "," + middleTreeOp.get().getId() : null,
					tableName, dictTable.getId());
			result.put("tab", tabTreeInit);
			return result;
		}catch (Exception e){
			throw new ConvertException("dict_table --> nreport_level_tree 转换错误", e);
		}
	}

	private NReportLevelTree initLevelTree(String treeName, Long timeMillis,Long parentId,String parentPath, String aliasName, Long tableId){
		return NReportLevelTree.builder().name(treeName).sourceName(DataSource.DATA_WAREHOUSE).sortValue(100)
				.createAt(timeMillis).updateAt(timeMillis).aliasName(aliasName).dictTableId(tableId).isValid(1)
				.parentPath(parentPath).parentId(parentId).build();
	}

}
