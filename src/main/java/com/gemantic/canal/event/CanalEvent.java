package com.gemantic.canal.event;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;


/**
 * ApplicationContext中的事件处理是通过ApplicationEvent类和ApplicationListener接口来提供的，
 * 通过ApplicationContext的publishEvent()方法发布到ApplicationListener;
 * 在这里包含三个角色：被发布的事件，事件的监听者和事件发布者。
 * 事件发布者在发布事件的时候通知事件的监听者。
 * spring的事件需要遵循以下流程：
 * （1）自定义事件：继承ApplicationEvent   当前类的作用
 * （2）定义事件监听器：实现ApplicationListener
 * （3）使用容器发布事件
 */
public abstract class CanalEvent extends ApplicationEvent {
    private int count = 0;

    public byte[] getByteSource() {
        return byteSource;
    }

    private byte[] byteSource;

    /**
     * 每个事件的资源定义
     * @param source
     */
    public CanalEvent(CanalEntry.Entry source) {
        super(source);
        this.byteSource = source.toByteArray();
    }

    /**
     * 暴露获取对象事件方法
     * 第二部监听器监听到事件后，需要获取事件对象资源进行处理
     * @return
     */
    public CanalEntry.Entry getEntry(){
      return  (CanalEntry.Entry) source;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void increment() {
        ++count;
    }
}
