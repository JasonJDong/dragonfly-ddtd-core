package org.dragonfly.ddtd.framework.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;

/**
 * 作用描述 任务节点
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/26
 **/
@Data
@Builder
class TaskNode {

    /**
     * 创建的子节点路径
     */
    private String path;
    /**
     * 子节点数据
     */
    private String data;
    /**
     * 子节点系统数据
     */
    private Stat node;
    /**
     * 节点模式
     */
    private CreateMode nodeMode;
    /**
     * 任务数据
     */
    private transient TaskNodeData taskNodeData;

    /**
     * 获取任务数据
     *
     * @return 任务数据
     */
    @Nullable
    public TaskNodeData getTaskNodeData() {
        if (this.taskNodeData != null) {
            return this.taskNodeData;
        }
        if (StringUtils.isNotBlank(this.data)) {
            try {
                return JSON.parseObject(this.data, TaskNodeData.class);
            } catch (JSONException e) {
                return null;
            }
        }
        return null;
    }
}
