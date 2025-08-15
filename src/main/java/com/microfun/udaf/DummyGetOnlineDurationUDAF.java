package com.microfun.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 自定义 UDAF，用于计算玩家在线时长。
 * UDAF 的主类，继承 AbstractGenericUDAFResolver，Hive 会首先调用这个类。
 */
@SuppressWarnings("deprecation")
public class DummyGetOnlineDurationUDAF extends AbstractGenericUDAFResolver {
    /**
     * Hive 调用此方法来确定用于处理查询的 Evaluator。
     * 这个方法负责检查输入参数的类型和数量。
     *
     * @param info 输入参数的类型信息
     * @return 返回一个 Evaluator 实例
     * @throws SemanticException 如果参数校验失败
     */
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        // 1. 校验参数个数，必须为1个
        if (info.length != 1) {
            throw new UDFArgumentException("函数需要且仅需要 1 个参数");
        }
        // 2. 校验参数类型，必须为整型 (int)
        if (!info[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE) ||
                !((org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo) info[0]).getPrimitiveCategory().equals(PrimitiveObjectInspector.PrimitiveCategory.INT)) {
            throw new UDFArgumentException("函数参数必须为 INT 类型");
        }
        return new OnlineDurationEvaluator();
    }


    /**
     * 实际执行聚合逻辑的 Evaluator 类。
     * 它的生命周期由 Hive 管理，每个 GROUP BY 键都会有一个独立的实例。
     */
    public static class OnlineDurationEvaluator extends GenericUDAFEvaluator {

        // 常量：10 分钟对应的秒数
        private static final int TEN_MINUTES_IN_SECONDS = 600;

        // --- Object Inspectors (OI) ---
        // OI用于解析不同阶段的数据结构
        // 1. 输入 OI：用于解析原始数据行中的 active_time (int)
        private transient PrimitiveObjectInspector inputOI;
        // 2. 中间结果 OI：用于序列化/反序列化部分聚合结果 (List<Integer>)
        private transient StandardListObjectInspector intermediateOI;


        /**
         * 聚合缓冲区的定义。这是 UDAF 的“状态”对象。
         * 它在聚合过程中存储所有的时间点。
         */
        @AggregationType(estimable = true)
        static class TimestampBuffer extends AbstractAggregationBuffer {
            List<Integer> timestamps;

            // 构造函数，初始化列表
            TimestampBuffer() {
                super();
                timestamps = new ArrayList<>();
            }

            // 返回此缓冲区的大致内存占用（以字节为单位）
            @Override
            public int estimate() {
                // 每个整数 4 字节
                return timestamps.size() * 4;
            }
        }


        /**
         * 初始化方法，在聚合的各个阶段开始时调用。
         * 主要作用是根据不同的运行模式 (Mode) 来初始化 ObjectInspector。
         *
         * @param m          运行模式 (PARTIAL1, COMPLETE, PARTIAL2, FINAL)
         * @param parameters 输入参数的 OI
         * @return 返回该阶段的输出 OI
         */
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            // PARTIAL1: Map 阶段，输入是原始数据。
            // COMPLETE: 整个聚合在单个节点上完成（没有 Map/Reduce）。
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                // 初始化输入 OI，用于解析原始的 int 类型时间点
                this.inputOI = (PrimitiveObjectInspector) parameters[0];
            }
            // PARTIAL2: Combiner 阶段，输入是 Map 阶段的部分聚合结果。
            // FINAL: Reduce 阶段，输入是 Combiner 或 Map 阶段的部分聚合结果。
            else {
                // 初始化中间结果 OI，用于解析 List<Integer>
                this.intermediateOI = (StandardListObjectInspector) parameters[0];
            }

            // 定义输出类型
            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                // 在 Map 和 Combiner 阶段，输出的是部分聚合结果，即一个时间点列表
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableIntObjectInspector
                );
            } else {
                // 在 Final 和 Complete 阶段，输出的是最终结果，即一个整数（总时长）
                return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
            }
        }

        /**
         * 创建一个新的聚合缓冲区实例。
         */
        @Override
        public @SuppressWarnings("deprecation") AggregationBuffer getNewAggregationBuffer() {
            return new TimestampBuffer();
        }

        /**
         * 重置聚合缓冲区，以便重用。
         */
        @Override
        public void reset(@SuppressWarnings("deprecation") AggregationBuffer agg) {
            ((TimestampBuffer) agg).timestamps.clear();
        }

        /**
         * Map阶段的核心方法，处理每一行输入数据。
         *
         * @param agg        当前聚合缓冲区
         * @param parameters 当前行的数据
         */
        @Override
        public void iterate(@SuppressWarnings("deprecation") AggregationBuffer agg, Object[] parameters) {
            if (parameters[0] != null) {
                // 从输入参数中提取 int 值
                int activeTime = PrimitiveObjectInspectorUtils.getInt(parameters[0], inputOI);
                // 将时间点添加到缓冲区的列表中
                ((TimestampBuffer) agg).timestamps.add(activeTime);
            }
        }

        @Override
        public Object terminatePartial(@SuppressWarnings("deprecation") AggregationBuffer agg) {
            List<Integer> timestamps = ((TimestampBuffer) agg).timestamps;
            // 创建一个新的列表来存放 IntWritable 对象
            ArrayList<IntWritable> result = new ArrayList<>(timestamps.size());
            for (Integer ts : timestamps) {
                result.add(new IntWritable(ts));
            }
            return result;
        }

        /**
         * Combiner 或 Reducer 阶段的核心方法，合并部分聚合结果。
         *
         * @param agg     当前聚合缓冲区
         * @param partial 另一个部分聚合的结果 (一个 List)
         */
        @Override
        public void merge(@SuppressWarnings("deprecation") AggregationBuffer agg, Object partial) {
            if (partial != null) {
                TimestampBuffer buffer = (TimestampBuffer) agg;
                List<?> partialTimestamps = intermediateOI.getList(partial);

                PrimitiveObjectInspector elementOI = (PrimitiveObjectInspector) intermediateOI.getListElementObjectInspector();

                if (partialTimestamps != null) {
                    for (Object timestampObj : partialTimestamps) {
                        int timestamp = PrimitiveObjectInspectorUtils.getInt(timestampObj, elementOI);
                        buffer.timestamps.add(timestamp);
                    }
                }
            }
        }

        /**
         * 返回最终聚合结果。在 Complete 或 Final 阶段结束时调用。
         */
        @Override
        public Object terminate(@SuppressWarnings("deprecation") AggregationBuffer agg) {
            List<Integer> timestamps = ((TimestampBuffer) agg).timestamps;

            // 1. 处理边缘情况
            if (timestamps == null || timestamps.isEmpty()) {
                return new IntWritable(0);
            }
            if (timestamps.size() == 1) {
                return new IntWritable(0);
            }

            // 2. 排序
            Collections.sort(timestamps);

            // 3. 计算总时长
            long totalDuration = 0;
            for (int l = 0, r; l < timestamps.size(); l = r) {
                r = l + 1;
                while (r < timestamps.size() && timestamps.get(r) - timestamps.get(r - 1) <= TEN_MINUTES_IN_SECONDS)
                    ++r;

                if (!Objects.equals(timestamps.get(l), timestamps.get(r - 1))) {
                    // 累加这个区间的时长
                    totalDuration += (timestamps.get(r - 1) - timestamps.get(l) + 1);
                }
            }

            return new IntWritable((int) totalDuration);
        }
    }
}