package com.microfun.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * 自定义 UDAF，用于计算玩家在线时长。
 */
@SuppressWarnings("deprecation")
public class BlockGetOnlineDurationUDAF extends AbstractGenericUDAFResolver {
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
        // 1. 校验参数个数，必须为 1 个
        if (info.length != 1)
            throw new UDFArgumentException("函数需要且仅需要 1 个参数");

        // 2. 校验参数类型，必须为整型 (int)
        if (!info[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE) ||
                !((org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo) info[0]).getPrimitiveCategory().equals(PrimitiveObjectInspector.PrimitiveCategory.INT)) {
            throw new UDFArgumentException("函数参数必须为 INT 类型");
        }

        return new BlockGetOnlineDurationEvaluator();
    }


    /**
     * 实际执行聚合逻辑的 Evaluator 类。
     * 它的生命周期由 Hive 管理，每个 GROUP BY 键都会有一个独立的实例。
     */
    public static class BlockGetOnlineDurationEvaluator extends GenericUDAFEvaluator {
        private static final int TOTAL_SECONDS = 86400;
        private static final int BLOCK_LENGTH = 600;
        private static final int BLOCK_COUNT = TOTAL_SECONDS / BLOCK_LENGTH;

        private transient PrimitiveObjectInspector inputOI;
        private transient PrimitiveObjectInspector intermediateOI;

        /**
         * 聚合缓冲区的定义。这是 UDAF 的“状态”对象。
         * 它在聚合过程中存储所有的时间点。
         */
        @AggregationType(estimable = true)
        static class BlockBuffer extends AbstractAggregationBuffer {
            // BitSet 用于标记每个 Block 是否有数据
            BitSet flags;
            // 数组用于按 [min, max, min, max, ...] 的顺序存储每个 Block 的边界值
            short[] offsets;

            private transient ByteBuffer serializationBuffer;
            private transient final BytesWritable partialResult;
            private transient final IntWritable finalResult;

            // 构造函数，初始化列表
            BlockBuffer() {
                super();
                this.flags = new BitSet(BLOCK_COUNT * 2);
                this.offsets = new short[BLOCK_COUNT * 2];

                this.partialResult = new BytesWritable();
                this.finalResult = new IntWritable();

                reset();
            }

            // reset 方法用于重置状态，而不是重新分配内存
            public void reset() {
                flags.clear();
            }

            // 将 block 连起来
            public void link() {
                for (int i = flags.nextSetBit(0);
                     i >= 0 && i + 2 < flags.size();
                     i = flags.nextSetBit(flags.nextClearBit(i + 2) - 1))
                    if (flags.get(i + 2)
                            && !flags.get(i + 1)
                            && offsets[i + 2] <= offsets[i + 1]
                    )
                        flags.set(i + 1);

            }

            // 返回此缓冲区的内存占用（以字节为单位）
            @Override
            public int estimate() {
                // short 数组大小 + BitSet 大小 + 其他开销
                return (BLOCK_COUNT * 2 * Short.BYTES) + (BLOCK_COUNT * 2 + 8 - 1) / 8 + 128;
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

            switch (m) {
                case PARTIAL1: // Map 阶段: 输入为原始数据，输出为中间聚合结果
                case COMPLETE: // 单机模式: 输入为原始数据，输出为最终结果
                    this.inputOI = (PrimitiveObjectInspector) parameters[0];
                    break;

                case PARTIAL2: // Combiner 阶段: 输入为中间结果，输出也是中间结果
                case FINAL:    // Reduce 阶段: 输入为中间结果，输出为最终结果
                    this.intermediateOI = (PrimitiveObjectInspector) parameters[0];
                    break;
            }

            switch (m) {
                case PARTIAL1: // Map 阶段的输出
                case PARTIAL2: // Combiner 阶段的输出
                    return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

                case FINAL:    // Reduce 阶段的输出
                case COMPLETE: // 单机模式的输出
                    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;

                default:
                    throw new HiveException("Unsupported mode: " + m);
            }
        }

        /**
         * 创建一个新的聚合缓冲区实例。
         */
        @Override
        public @SuppressWarnings("deprecation") AggregationBuffer getNewAggregationBuffer() {
            return new BlockBuffer();
        }

        /**
         * 重置聚合缓冲区，以便重用。
         */
        @Override
        public void reset(@SuppressWarnings("deprecation") AggregationBuffer agg) {
            ((BlockBuffer) agg).reset();
        }

        /**
         * Map 阶段的核心方法，处理每一行输入数据。
         *
         * @param agg        当前聚合缓冲区
         * @param parameters 当前行的数据
         */
        @Override
        public void iterate(@SuppressWarnings("deprecation") AggregationBuffer agg, Object[] parameters) {
            if (agg == null || parameters[0] == null)
                return;

            BlockBuffer buffer = (BlockBuffer) agg;
            BitSet flags = buffer.flags;
            short[] offsets = buffer.offsets;

            int timeStamp = PrimitiveObjectInspectorUtils.getInt(parameters[0], inputOI);

            int blockId = timeStamp / BLOCK_LENGTH;
            short blockOffset = (short) (timeStamp % BLOCK_LENGTH);
            int minIndex = blockId * 2, maxIndex = blockId * 2 + 1;

            if (flags.get(blockId * 2)) {
                offsets[minIndex] = (short) Math.min(offsets[minIndex], blockOffset);
                offsets[maxIndex] = (short) Math.max(offsets[maxIndex], blockOffset);
            } else {
                flags.set(blockId * 2);
                offsets[minIndex] = blockOffset;
                offsets[maxIndex] = blockOffset;
            }
        }

        /**
         * Map/Combiner 阶段结束时调用，返回部分聚合结果。
         *
         * @param agg 当前聚合缓冲区
         * @return 一个包含缓冲区数据的对象
         */
        @Override
        public Object terminatePartial(@SuppressWarnings("deprecation") AggregationBuffer agg) {
            if (agg == null)
                return null;

            BlockBuffer buffer = (BlockBuffer) agg;
            buffer.link();

            BitSet flags = buffer.flags;
            short[] offsets = buffer.offsets;

            short blockCount = 0;
            for (int i = flags.nextSetBit(0); i >= 0; i = flags.nextSetBit(flags.nextClearBit(i))) {
                ++blockCount;
            }

            if (blockCount == 0)
                return null;

            long[] flagsLongs = flags.toLongArray();

            // 计算所需缓冲区大小
            int requiredSize = 4 + flagsLongs.length * Long.BYTES + blockCount * 2 * Short.BYTES;

            // 如果可重用 buffer 不存在或容量不足，则重新分配
            if (buffer.serializationBuffer == null || buffer.serializationBuffer.capacity() < requiredSize)
                buffer.serializationBuffer = ByteBuffer.allocate(requiredSize);
            ByteBuffer bb = buffer.serializationBuffer;
            ((Buffer) bb).position(0);

            bb.putInt(flagsLongs.length);
            for (long l : flagsLongs)
                bb.putLong(l);

            for (int l = flags.nextSetBit(0), r; l >= 0; l = flags.nextSetBit(r)) {
                r = flags.nextClearBit(l);

                bb.putShort(offsets[l]);
                bb.putShort(offsets[r]);
            }

            buffer.partialResult.set(bb.array(), 0, bb.position());
            return buffer.partialResult;
        }

        /**
         * Combiner 或 Reducer 阶段的核心方法，合并部分聚合结果。
         *
         * @param agg     当前聚合缓冲区
         * @param partial 另一个部分聚合的结果。
         */
        @Override
        public void merge(@SuppressWarnings("deprecation") AggregationBuffer agg, Object partial) {
            if (agg == null || partial == null)
                return;

            BlockBuffer buffer = (BlockBuffer) agg;
            BitSet flags = buffer.flags;
            short[] offsets = buffer.offsets;

            BytesWritable partialBytes = (BytesWritable) intermediateOI.getPrimitiveWritableObject(partial);
            ByteBuffer bb = ByteBuffer.wrap(partialBytes.getBytes(), 0, partialBytes.getLength());

            int flagsLength = bb.getInt();
            long[] flagsLongs = new long[flagsLength];
            for (int i = 0; i < flagsLength; i++)
                flagsLongs[i] = bb.getLong();
            BitSet partialFlags = BitSet.valueOf(flagsLongs);

            for (int l = partialFlags.nextSetBit(0), r; l >= 0; l = partialFlags.nextSetBit(r)) {
                r = partialFlags.nextClearBit(l);

                short minValue = bb.getShort(), maxValue = bb.getShort();
                offsets[l] = flags.get(l) ? (short) Math.min(offsets[l], minValue) : minValue;
                offsets[r] = flags.get(r - 1) ? (short) Math.max(offsets[r], maxValue) : maxValue;
            }

            flags.or(partialFlags);
        }

        /**
         * 返回最终聚合结果。在 Complete 或 Final 阶段结束时调用。
         */
        @Override
        public Object terminate(@SuppressWarnings("deprecation") AggregationBuffer agg) {
            if (agg == null)
                return new IntWritable(0);

            BlockBuffer buffer = (BlockBuffer) agg;
            buffer.finalResult.set(getDuration(buffer));
            return buffer.finalResult;
        }

        private int getDuration(BlockBuffer buffer) {
            buffer.link();

            BitSet flags = buffer.flags;
            short[] offsets = buffer.offsets;

            int blockCount = 0, offsetSum = 0;
            for (int l = flags.nextSetBit(0), r; l >= 0; l = flags.nextSetBit(r)) {
                r = flags.nextClearBit(l);

                if (l + 1 != r || offsets[l] != offsets[r]) {
                    blockCount += ((r - 1) - l) >> 1;
                    offsetSum += offsets[r] - offsets[l] + 1;
                }
            }

            return blockCount * BLOCK_LENGTH + offsetSum;
        }
    }
}
