package com.gemantic.canal.event;

import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * 更新事件
 */
public class UpdateCanalEvent extends CanalEvent {

    public UpdateCanalEvent(CanalEntry.Entry source) {
        super(source);
    }
}
