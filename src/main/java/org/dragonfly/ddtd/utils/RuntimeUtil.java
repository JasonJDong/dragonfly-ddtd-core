package org.dragonfly.ddtd.utils;

import cn.hutool.system.oshi.CpuInfo;
import cn.hutool.system.oshi.OshiUtil;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;

/**
 * 作用描述 运行时工具
 *
 * @author jian.dong1
 * @version 1.0
 * @date 2022/2/16
 **/
public class RuntimeUtil {
    /**
     * cpu负载
     *
     * @return cpu负载，百分比
     */
    public static double cpuLoad() {

        CpuInfo cpuInfo = OshiUtil.getCpuInfo();
        double free = cpuInfo.getFree();
        return 100.0d - free;
    }

    /**
     * cpu负载文本输出
     *
     * @return cpu负载文本输出
     */
    public static String cpuLoadString() {

        DecimalFormat format = new DecimalFormat("0.00");
        return "cpu利用率：" + format.format(cpuLoad());
    }

    /**
     * 内存负载
     *
     * @return 内存负载，百分比
     */
    public static double memoryLoad() {
        return BigDecimal.valueOf(OshiUtil.getMemory().getTotal() - OshiUtil.getMemory().getAvailable())
                .divide(BigDecimal.valueOf(OshiUtil.getMemory().getTotal()), 2, RoundingMode.HALF_UP)
                .multiply(BigDecimal.valueOf(100))
                .doubleValue();
    }

    /**
     * 内存负载文本输出
     *
     * @return 内存负载文本输出
     */
    public static String memoryLoadString() {

        DecimalFormat format = new DecimalFormat("0.00");
        return "内存使用率：" + format.format(memoryLoad());
    }
}
