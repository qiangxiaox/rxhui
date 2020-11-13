package com.gemantic.canal.event;

import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * 插入事件
 */
public class InsertCanalEvent extends CanalEvent {

    public InsertCanalEvent(CanalEntry.Entry source) {
        super(source);
    }
}
