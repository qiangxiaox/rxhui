package com.gemantic.report.model;

import com.gemantic.warehouse.model.DictColumn;
import com.gemantic.warehouse.model.DictTable;
import com.gemantic.warehouse.model.SchemaColumns;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * createed By xiaoqiang
 * 2019/12/9 14:02
 */
@Slf4j
public class TransferDataSnapshoot {

	//手动维护的表字典信息
	@Getter
	private List<DictTable> dictTable;

	@Getter
	private Map<String, DictTable> tableMap;

	//数据库维护的字段字典表信息
	@Getter
	private List<CompositeColumn> compositeColumns;

	public TransferDataSnapshoot() {
	}

	public TransferDataSnapshoot(List<DictTable> insertTable, List<DictColumn> dictColumn,
	                             List<SchemaColumns> schemaColumns, List<DictTable> allTable) {
		this.dictTable = insertTable;
		compositeColumns = Lists.newArrayList();
		//将数据库维护的字段信息和人工维护的字段信息进行组合，以人工维护的为标准
		for (DictColumn column : dictColumn){
			String tableName = column.getTableName();
			String columnName = column.getColumnName();
			SchemaColumns schemaColumn = null;
			try {
				schemaColumn = schemaColumns.stream()
						.filter(obj -> obj.getTableName().equals(tableName)
								&& obj.getColumnName().equalsIgnoreCase(columnName)).findFirst().get();
			}catch (Exception e){
				log.error("tableName={}, columnName={} 匹配SchemaColumn错误",tableName, columnName);
			}

			CompositeColumn compositeColumn = new CompositeColumn(columnName, tableName, column, schemaColumn);
			compositeColumns.add(compositeColumn);
		}
		tableMap = allTable.stream().collect(Collectors.toMap(DictTable::getTableName, Function.identity()));
	}

}
