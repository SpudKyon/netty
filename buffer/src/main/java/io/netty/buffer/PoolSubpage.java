/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.buffer;

import java.util.concurrent.locks.ReentrantLock;

import static io.netty.buffer.PoolChunk.RUN_OFFSET_SHIFT;
import static io.netty.buffer.PoolChunk.SIZE_SHIFT;
import static io.netty.buffer.PoolChunk.IS_USED_SHIFT;
import static io.netty.buffer.PoolChunk.IS_SUBPAGE_SHIFT;

final class PoolSubpage<T> implements PoolSubpageMetric {

    final PoolChunk<T> chunk;
    final int elemSize;
    private final int pageShifts;
    private final int runOffset;
    private final int runSize;
    private final long[] bitmap;
    private final int bitmapLength;
    private final int maxNumElems;
    final int headIndex;

    PoolSubpage<T> prev;
    PoolSubpage<T> next;

    boolean doNotDestroy;
    private int nextAvail;
    private int numAvail;

    final ReentrantLock lock;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    /** Special constructor that creates a linked list head */
    PoolSubpage(int headIndex) {
        chunk = null;
        lock = new ReentrantLock();
        pageShifts = -1;
        runOffset = -1;
        elemSize = -1;
        runSize = -1;
        bitmap = null;
        bitmapLength = -1;
        maxNumElems = 0;
        this.headIndex = headIndex;
    }

    PoolSubpage(PoolSubpage<T> head, PoolChunk<T> chunk, int pageShifts, int runOffset, int runSize, int elemSize) {
        this.headIndex = head.headIndex;
        this.chunk = chunk;
        this.pageShifts = pageShifts;
        this.runOffset = runOffset;
        this.runSize = runSize;
        this.elemSize = elemSize;

        doNotDestroy = true;

        maxNumElems = numAvail = runSize / elemSize;
        int bitmapLength = maxNumElems >>> 6;
        if ((maxNumElems & 63) != 0) {
            bitmapLength ++;
        }
        this.bitmapLength = bitmapLength;
        bitmap = new long[bitmapLength];
        nextAvail = 0;

        lock = null;
        addToPool(head);
    }

    /**
     * Returns the bitmap index of the subpage allocation.
     */
    // 分配一个可用的位图索引
    long allocate() {
        // 检查是否有可用的元素，或者是否允许销毁
        if (numAvail == 0 || !doNotDestroy) {
            return -1; // 如果没有可用元素或不允许销毁，返回 -1
        }

        // 获取下一个可用的位图索引
        final int bitmapIdx = getNextAvail();
        // 检查位图索引是否有效
        if (bitmapIdx < 0) {
            removeFromPool(); // 如果位图索引无效，移除子页面以防止重复错误
            throw new AssertionError("No next available bitmap index found (bitmapIdx = " + bitmapIdx + "), " +
                    "even though there are supposed to be (numAvail = " + numAvail + ") " +
                    "out of (maxNumElems = " + maxNumElems + ") available indexes.");
        }
        // 计算位图索引的行和列
        int q = bitmapIdx >>> 6; // 行索引
        int r = bitmapIdx & 63;   // 列索引
        // 确保该位图位置未被占用
        assert (bitmap[q] >>> r & 1) == 0;
        // 标记该位图位置为已占用
        bitmap[q] |= 1L << r;

        // 减少可用元素的数量，并检查是否为零
        if (-- numAvail == 0) {
            removeFromPool(); // 如果没有可用元素，移除子页面
        }

        // 返回位图索引的句柄
        return toHandle(bitmapIdx);
    }

    /**
     * @return {@code true} if this subpage is in use.
     *         {@code false} if this subpage is not used by its chunk and thus it's OK to be released.
     */
    boolean free(PoolSubpage<T> head, int bitmapIdx) {
        int q = bitmapIdx >>> 6;
        int r = bitmapIdx & 63;
        assert (bitmap[q] >>> r & 1) != 0;
        bitmap[q] ^= 1L << r;

        setNextAvail(bitmapIdx);

        if (numAvail ++ == 0) {
            addToPool(head);
            /* When maxNumElems == 1, the maximum numAvail is also 1.
             * Each of these PoolSubpages will go in here when they do free operation.
             * If they return true directly from here, then the rest of the code will be unreachable
             * and they will not actually be recycled. So return true only on maxNumElems > 1. */
            if (maxNumElems > 1) {
                return true;
            }
        }

        if (numAvail != maxNumElems) {
            return true;
        } else {
            // Subpage not in use (numAvail == maxNumElems)
            if (prev == next) {
                // Do not remove if this subpage is the only one left in the pool.
                return true;
            }

            // Remove this subpage from the pool if there are other subpages left in the pool.
            doNotDestroy = false;
            removeFromPool();
            return false;
        }
    }

    private void addToPool(PoolSubpage<T> head) {
        assert prev == null && next == null;
        prev = head;
        next = head.next;
        next.prev = this;
        head.next = this;
    }

    private void removeFromPool() {
        assert prev != null && next != null;
        prev.next = next;
        next.prev = prev;
        next = null;
        prev = null;
    }

    private void setNextAvail(int bitmapIdx) {
        nextAvail = bitmapIdx;
    }

    // 获取下一个可用的索引
    private int getNextAvail() {
        // 读取当前的下一个可用索引
        int nextAvail = this.nextAvail;
        // 如果下一个可用索引有效（大于等于0）
        if (nextAvail >= 0) {
            // 将下一个可用索引重置为-1，表示已被使用
            this.nextAvail = -1;
            // 返回当前的下一个可用索引
            return nextAvail;
        }
        // 如果没有有效的下一个可用索引，调用查找方法
        return findNextAvail();
    }

    // 查找下一个可用的索引
    private int findNextAvail() {
        // 遍历位图的每一项
        for (int i = 0; i < bitmapLength; i ++) {
            // 获取当前位图的值
            long bits = bitmap[i];
            // 如果当前位图中有可用的位（即不是全1）
            if (~bits != 0) {
                // 调用辅助方法查找具体的可用索引
                return findNextAvail0(i, bits);
            }
        }
        // 如果没有找到可用的索引，返回-1
        return -1;
    }

    // 在指定的位图中查找下一个可用的索引
    private int findNextAvail0(int i, long bits) {
        // 计算基值，基于当前位图的索引
        final int baseVal = i << 6;
        // 遍历64位
        for (int j = 0; j < 64; j ++) {
            // 检查当前位是否可用（即为0）
            if ((bits & 1) == 0) {
                // 计算具体的可用索引
                int val = baseVal | j;
                // 如果可用索引小于最大元素数量，返回该索引
                if (val < maxNumElems) {
                    return val;
                } else {
                    // 如果超出范围，退出循环
                    break;
                }
            }
            // 右移位图，检查下一个位
            bits >>>= 1;
        }
        // 如果没有找到可用的索引，返回-1
        return -1;
    }

    private long toHandle(int bitmapIdx) {
        int pages = runSize >> pageShifts;
        return (long) runOffset << RUN_OFFSET_SHIFT
               | (long) pages << SIZE_SHIFT
               | 1L << IS_USED_SHIFT
               | 1L << IS_SUBPAGE_SHIFT
               | bitmapIdx;
    }

    @Override
    public String toString() {
        final int numAvail;
        if (chunk == null) {
            // This is the head so there is no need to synchronize at all as these never change.
            numAvail = 0;
        } else {
            final boolean doNotDestroy;
            PoolSubpage<T> head = chunk.arena.smallSubpagePools[headIndex];
            head.lock();
            try {
                doNotDestroy = this.doNotDestroy;
                numAvail = this.numAvail;
            } finally {
                head.unlock();
            }
            if (!doNotDestroy) {
                // Not used for creating the String.
                return "(" + runOffset + ": not in use)";
            }
        }

        return "(" + this.runOffset + ": " + (this.maxNumElems - numAvail) + '/' + this.maxNumElems +
                ", offset: " + this.runOffset + ", length: " + this.runSize + ", elemSize: " + this.elemSize + ')';
    }

    @Override
    public int maxNumElements() {
        return maxNumElems;
    }

    @Override
    public int numAvailable() {
        if (chunk == null) {
            // It's the head.
            return 0;
        }
        PoolSubpage<T> head = chunk.arena.smallSubpagePools[headIndex];
        head.lock();
        try {
            return numAvail;
        } finally {
            head.unlock();
        }
    }

    @Override
    public int elementSize() {
        return elemSize;
    }

    @Override
    public int pageSize() {
        return 1 << pageShifts;
    }

    boolean isDoNotDestroy() {
        if (chunk == null) {
            // It's the head.
            return true;
        }
        PoolSubpage<T> head = chunk.arena.smallSubpagePools[headIndex];
        head.lock();
        try {
            return doNotDestroy;
        } finally {
            head.unlock();
        }
    }

    void destroy() {
        if (chunk != null) {
            chunk.destroy();
        }
    }

    void lock() {
        lock.lock();
    }

    void unlock() {
        lock.unlock();
    }
}
