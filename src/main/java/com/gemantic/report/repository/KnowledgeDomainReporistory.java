package com.gemantic.report.repository;

import com.gemantic.dict.model.Dict;

import java.util.List;

/**
 * createed By xiaoqiang
 * 2019/12/12 16:46
 */
public interface KnowledgeDomainReporistory {

	void saveDict(List<Dict> dictList);

	List<Dict> findPropDict();
}
