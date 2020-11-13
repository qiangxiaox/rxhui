package com.gemantic.canal.event;

import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * 删除事件
 */
public class DeleteCanalEvent extends CanalEvent {

    public DeleteCanalEvent(CanalEntry.Entry source) {
        super(source);
    }
}
