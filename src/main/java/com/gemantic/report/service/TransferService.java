package com.gemantic.report.service;

import com.gemantic.report.model.TransferDataSnapshoot;

/**
 * createed By xiaoqiang
 * 2019/12/7 13:46
 */
public interface TransferService {

	void transferToReport() throws Exception;

	void transferPointTime(Long columnStart, Long tableStart);
}
