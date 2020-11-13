package com.gemantic.canal.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * createed By xiaoqiang
 * 2020/11/13 10:43
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogExtraEntry {

	private byte[] dataSource;

	private int count;

	private String javaType;
}
