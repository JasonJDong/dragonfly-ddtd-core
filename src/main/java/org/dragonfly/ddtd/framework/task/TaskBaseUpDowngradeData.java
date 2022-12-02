package org.dragonfly.ddtd.framework.task;

import org.dragonfly.ddtd.framework.entity.ITaskContext;
import lombok.Data;

/**
 * 作用描述 任务升降级数据中心
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/1/28
 **/
@Data
public class TaskBaseUpDowngradeData {

    private final ITaskContext taskContext;
}
