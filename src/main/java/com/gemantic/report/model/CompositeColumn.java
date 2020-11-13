package com.gemantic.report.model;

import com.gemantic.warehouse.model.DictColumn;
import com.gemantic.warehouse.model.SchemaColumns;
import lombok.Getter;

/**
 * createed By xiaoqiang
 * 2019/12/11 11:35
 */
public class CompositeColumn {
	//字段名
	@Getter
	private String columnName;
	//表名
	@Getter
	private String tableName;
	@Getter
	private DictColumn dictColumn;
	@Getter
	private SchemaColumns schemaColumns;
	//业务唯一键

	public CompositeColumn(String columnName, String tableName, DictColumn dictColumn,
	                       SchemaColumns schemaColumns) {
		this.columnName = columnName;
		this.tableName = tableName;
		this.dictColumn = dictColumn;
		this.schemaColumns = schemaColumns;

	}

	public CompositeColumn() {}
}
