package org.dragonfly.ddtd.framework.exception;

import cn.hutool.core.net.NetUtil;
import com.alibaba.fastjson.JSON;
import org.dragonfly.ddtd.framework.entity.ITaskContext;
import org.dragonfly.ddtd.framework.task.ITaskMeta;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

/**
 * 作用描述 重复任务创建异常
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/2/7
 **/
@Data
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class DuplicatedTaskCreationException extends RuntimeException {
    /**
     * 重复任务
     */
    private final ITaskMeta taskMeta;
    /**
     * 重复任务上下文
     */
    private final ITaskContext taskContext;

    @Override
    public String getMessage() {
        return String.format("当前实例：%s，重复任务：%s，任务名称：%s，任务版本：%s，任务上下文：%s",
                NetUtil.getLocalhostStr(),
                getTaskMeta().getDescription(),
                getTaskMeta().getName(),
                getTaskMeta().getVersion(),
                JSON.toJSONString(getTaskContext())
        );
    }
}
