package org.dragonfly.ddtd.framework.report;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import org.dragonfly.ddtd.framework.entity.ITaskContext;
import org.dragonfly.ddtd.framework.task.ITaskWorker;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 作用描述 默认系统报告器
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/2/17
 **/
@Slf4j
public class DefaultTaskSystemReporter implements ITaskSystemReporter {

    @Override
    public void report(String baseMsg, ITaskWorker<?> taskWorker, ITaskContext taskContext, TaskProcessStage stage, Object... extendObjs) {
        if (ArrayUtils.isEmpty(extendObjs)) {
            log.info("{}，任务：{}，上下文：{}, 处理阶段：{}",
                    baseMsg,
                    taskWorker.getDescription(),
                    Optional.ofNullable(taskContext).map(JSON::toJSONString).orElse(StrUtil.EMPTY),
                    stage.name()
            );
        } else {
            log.info("{}，任务：{}，上下文：{}, 处理阶段：{}，额外数据：{}",
                    baseMsg,
                    taskWorker.getDescription(),
                    Optional.ofNullable(taskContext).map(JSON::toJSONString).orElse(StrUtil.EMPTY),
                    stage.name(),
                    Stream.of(extendObjs).map(JSON::toJSONString).collect(Collectors.joining(StrUtil.COMMA))
            );
        }
    }

    @Override
    public void report(String baseMsg, ITaskWorker<?> taskWorker, ITaskContext taskContext, TaskProcessStage stage, Exception error, Object... extendObjs) {
        if (ArrayUtils.isEmpty(extendObjs)) {
            log.info("{}，任务：{}，上下文：{}, 处理阶段：{}",
                    baseMsg,
                    taskWorker.getDescription(),
                    Optional.ofNullable(taskContext).map(JSON::toJSONString).orElse(StrUtil.EMPTY),
                    stage.name(),
                    error
            );
        } else {
            log.info("{}，任务：{}，上下文：{}, 处理阶段：{}，额外数据：{}",
                    baseMsg,
                    taskWorker.getDescription(),
                    Optional.ofNullable(taskContext).map(JSON::toJSONString).orElse(StrUtil.EMPTY),
                    stage.name(),
                    Stream.of(extendObjs).map(JSON::toJSONString).collect(Collectors.joining(StrUtil.COMMA)),
                    error
            );
        }
    }
}
