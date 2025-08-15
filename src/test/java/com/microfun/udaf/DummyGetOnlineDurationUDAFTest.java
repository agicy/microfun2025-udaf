package com.microfun.udaf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * GetOnlineDurationUDAF 的单元测试类。
 * 专注于测试核心业务逻辑，即 OnlineDurationEvaluator 的行为。
 */
@DisplayName("在线时长 UDAF 核心逻辑测试")
class DummyGetOnlineDurationUDAFTest {

    // UDAF 的核心逻辑执行器
    private DummyGetOnlineDurationUDAF.OnlineDurationEvaluator evaluator;
    // 聚合数据缓冲区
    @SuppressWarnings("deprecation")
    private GenericUDAFEvaluator.AggregationBuffer buffer;

    /**
     * 在每个测试方法执行前运行的设置方法。
     * 负责初始化 Evaluator 和 Buffer，确保测试环境的纯净。
     */
    @BeforeEach
    void setUp() throws HiveException {
        evaluator = new DummyGetOnlineDurationUDAF.OnlineDurationEvaluator();

        // 模拟 Hive 的初始化过程。
        // 对于 UDAF 来说，init() 是必须调用的，因为它会设置内部状态（例如 ObjectInspector）。
        // 我们模拟一个最简单的场景：在单个节点上完成所有计算（Mode.COMPLETE）。
        // 输入参数是一个整型（INT）。
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, new ObjectInspector[]{intOI});

        // 获取一个新的、干净的聚合缓冲区
        buffer = evaluator.getNewAggregationBuffer();
    }

    /**
     * 辅助方法，用于模拟 Hive 向 UDAF 喂入一系列时间点数据。
     *
     * @param timestamps 时间点列表
     */
    private void processTimestamps(List<Integer> timestamps) {
        for (Integer ts : timestamps) {
            // Hive 的数据是以 Object 数组的形式传入的，并且基础类型会被包装成 Writable 对象。
            evaluator.iterate(buffer, new Object[]{new IntWritable(ts)});
        }
    }

    /**
     * 辅助方法，用于获取最终的计算结果。
     *
     * @return 计算出的总时长（int 类型）
     */
    private int getFinalResult() {
        // 调用 terminate 方法获取最终结果
        IntWritable result = (IntWritable) evaluator.terminate(buffer);
        // 如果结果为 null，返回 0，否则返回其 int 值
        return result == null ? 0 : result.get();
    }

    @Test
    @DisplayName("测试 PDF 文档中的标准示例")
    void testStandardCaseFromDocument() {
        // (99 - 2 + 1) + (1101 - 1000 + 1) + (20651 - 20051 + 1) = 98 + 102 + 601 = 801
        List<Integer> timestamps = Arrays.asList(2, 3, 99, 1000, 1100, 1101, 9999, 20051, 20651);
        processTimestamps(timestamps);
        int duration = getFinalResult();
        assertEquals(801, duration, "标准示例的计算结果应为 801");
    }

    @Test
    @DisplayName("测试无序输入，验证排序逻辑是否正常工作")
    void testUnorderedInput() {
        // 与标准示例数据相同，但顺序打乱
        List<Integer> timestamps = Arrays.asList(1101, 99, 2, 20651, 1000, 9999, 1100, 20051, 3);
        processTimestamps(timestamps);
        int duration = getFinalResult();
        assertEquals(801, duration, "乱序输入的计算结果应与有序输入相同");
    }

    @Test
    @DisplayName("测试边缘情况：没有输入数据")
    void testEmptyInput() {
        processTimestamps(Collections.emptyList());

        int duration = getFinalResult();
        assertEquals(0, duration, "没有输入时，时长应为 0");
    }

    @Test
    @DisplayName("测试边缘情况：只有一个时间点")
    void testSingleTimestamp() {
        processTimestamps(Collections.singletonList(100));

        int duration = getFinalResult();
        assertEquals(0, duration, "单个时间点的时长应为 0");
    }

    @Test
    @DisplayName("测试所有时间点都在一个连续区间内")
    void testSingleContinuousInterval() {
        // 所有时间点都在 10 分钟（600 秒）内
        List<Integer> timestamps = Arrays.asList(100, 200, 300, 500, 700);
        processTimestamps(timestamps);

        // 期望结果：700 - 100 + 1 = 601
        int duration = getFinalResult();
        assertEquals(601, duration, "单个连续区间的时长计算应正确");
    }

    @Test
    @DisplayName("测试所有时间点都构成独立的区间")
    void testAllDistinctIntervals() {
        // 每两个时间点之间都相差超过 600 秒
        List<Integer> timestamps = Arrays.asList(100, 1000, 2000, 3000);
        processTimestamps(timestamps);

        // 期望结果：0
        int duration = getFinalResult();
        assertEquals(0, duration, "每个点都是独立区间的总时长应为 0");
    }

    @Test
    @DisplayName("测试边界条件：两个时间点恰好相差 10 分钟（600 秒）")
    void testBoundaryExactlyTenMinutes() {
        // 100 和 700 相差 600，应该被算作同一个区间
        List<Integer> timestamps = Arrays.asList(100, 700);
        processTimestamps(timestamps);

        // 期望结果：700 - 100 + 1 = 601
        int duration = getFinalResult();
        assertEquals(601, duration, "相差恰好 600 秒应视为同一区间");
    }

    @Test
    @DisplayName("测试边界条件：两个时间点相差超过 10 分钟多 1 秒")
    void testBoundaryOverTenMinutes() {
        // 100 和 701 相差 601，应该被算作两个独立区间
        List<Integer> timestamps = Arrays.asList(100, 701);
        processTimestamps(timestamps);
        // 期望结果：1 + 1 = 2
        int duration = getFinalResult();
        assertEquals(0, duration, "相差 601 秒应视为两个独立区间");
    }

    @Test
    @DisplayName("测试 merge 逻辑 - 模拟 MapReduce 过程")
    void testMergeLogic() throws HiveException {
        // 场景：模拟两个 mapper 的部分结果，然后在 reducer 中合并

        // 1. 第一个 mapper 的处理
        @SuppressWarnings("deprecation")
        GenericUDAFEvaluator.AggregationBuffer buffer1 = evaluator.getNewAggregationBuffer();
        evaluator.iterate(buffer1, new Object[]{new IntWritable(100)});
        evaluator.iterate(buffer1, new Object[]{new IntWritable(200)});

        // 2. 第二个 mapper 的处理
        @SuppressWarnings("deprecation")
        GenericUDAFEvaluator.AggregationBuffer buffer2 = evaluator.getNewAggregationBuffer();
        evaluator.iterate(buffer2, new Object[]{new IntWritable(900)});
        evaluator.iterate(buffer2, new Object[]{new IntWritable(1000)});

        // 3. 获取两个 mapper 的部分聚合结果
        Object partialResult1 = evaluator.terminatePartial(buffer1);
        Object partialResult2 = evaluator.terminatePartial(buffer2);
        assertNotNull(partialResult1);
        assertNotNull(partialResult2);

        // 4. 在 reducer 中合并
        // 需要重新初始化 evaluator 来模拟 reducer 阶段
        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableIntObjectInspector
        );
        evaluator.init(GenericUDAFEvaluator.Mode.FINAL, new ObjectInspector[]{listOI});

        @SuppressWarnings("deprecation")
        GenericUDAFEvaluator.AggregationBuffer finalBuffer = evaluator.getNewAggregationBuffer();
        evaluator.merge(finalBuffer, partialResult1);

        // 5. 验证合并后和最终结果
        int intermediateDuration = ((IntWritable) evaluator.terminate(finalBuffer)).get();
        assertEquals(101, intermediateDuration, "合并第一个部分结果后计算正确");

        evaluator.merge(finalBuffer, partialResult2);
        int finalDuration = ((IntWritable) evaluator.terminate(finalBuffer)).get();
        // (200 - 100 + 1) + (1000 - 900 + 1) = 101 + 101 = 202
        assertEquals(202, finalDuration, "合并所有部分结果后计算正确");
    }
}
