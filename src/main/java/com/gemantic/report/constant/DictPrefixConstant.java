package com.gemantic.report.constant;

import com.gemantic.report.repository.MetadataThemeRepository;
import com.gemantic.springcloud.model.Response;
import com.gemantic.warehouse.model.DictPrefix;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class DictPrefixConstant {

  private static final String entity_type = "实体";
  @Resource
  private MetadataThemeRepository metadataThemeRepository;

  private List<DictPrefix> dictPrefix = Lists.newArrayList();

  //key是前缀， value是名称
  private Map<String, String> dictPrefixMap = Maps.newHashMap();
  //key是实体标签，value是对应的信息
  private Map<String, DictPrefix> entityMap = Maps.newHashMap();

  @EventListener
  public void init(ContextRefreshedEvent event) {
    updateData();
  }

  public void updateData(){
    if(dictPrefix.size() > 0){
        this.dictPrefix.clear();
        this.dictPrefixMap.clear();
    }
    ResponseEntity<Response<List<DictPrefix>>> responseEntity = metadataThemeRepository.selectPrefix();
    List<DictPrefix> prefixes = responseEntity.getBody().getData();
    dictPrefix.addAll(prefixes);
    Map<String, String> stringMap = prefixes.stream().collect(Collectors.toMap(DictPrefix::getTablePrefix, DictPrefix::getTablePrefixName));
    dictPrefixMap.putAll(stringMap);
    Map<String, DictPrefix> entityPrefixMap = prefixes.stream().filter(obj -> entity_type.equals(obj.getPrefixType()))
            .collect(Collectors.toMap(DictPrefix::getTablePrefix, Function.identity()));
    entityMap.putAll(entityPrefixMap);
  }

  public List<DictPrefix> getDictPrefix(){
    return dictPrefix;
  }

  public Map<String, DictPrefix> getEntityMap() {
    return entityMap;
  }

  public Map<String, String> getDictPrefixMap() {
    return dictPrefixMap;
  }
}
