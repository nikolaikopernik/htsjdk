/*
 * The MIT License
 *
 * Copyright (c) 2009 The Broad Institute
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package htsjdk.samtools.util;

import htsjdk.samtools.BAMIndexer;
import htsjdk.samtools.ParallelBAMIndexer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Parallel implementation of BAM file compression.
 *
 * The main idea is to compress blocks of a BAM file asynchronously. After a next 64K block filled,
 * it sends to the first available processor core and continues to fill a next 64K block of data.
 * After compressing each thread save compressed block to the disk in input order.
 *
 * For first tests this approach increase a writing speed by 70% and a common speed (reading, metric,
 * saving) increased by 43% (but it depends on CPU count, HDD speed etc)
 *
 * @see AbstractCompressedOutputStream
 * @author Nikolai_Bogdanov@epam.com
 */
public class ParallelBlockCompressedOutputStream
        extends AbstractCompressedOutputStream
{
    /**
     * All blocks must be written consistently, so every new deflates block has its own index from
     * {@link #currentBlockIdx}, and it will be written separatly by its index only then
     * {@link #nextBlockIdxToWrite} == block index.
     */
    protected int currentBlockIdx = 0;
    protected volatile int nextBlockIdxToWrite = 0;

    private ParallelDeflatersPool pool;
    private ParallelBAMIndexer indexer;

    /**
     * Uses default compression level, which is 5 unless changed by setCompressionLevel
     */
    public ParallelBlockCompressedOutputStream(final File file) {
        this(file, defaultCompressionLevel);
    }

    /**
     * Prepare to compress at the given compression level
     * @param compressionLevel 1 <= compressionLevel <= 9
     */
    public ParallelBlockCompressedOutputStream(final File file, final int compressionLevel) {
        super(file, compressionLevel);
        pool =  new ParallelDeflatersPool(this, Runtime.getRuntime().availableProcessors(), compressionLevel);
    }

    /**
     * Constructors that take output streams
     * file may be null
     */
    public ParallelBlockCompressedOutputStream(final OutputStream os, final File file) {
        this(os, file, defaultCompressionLevel);
    }

    public ParallelBlockCompressedOutputStream(final OutputStream os, final File file, final int compressionLevel) {
        super(os, file, compressionLevel);
        pool =  new ParallelDeflatersPool(this, Runtime.getRuntime().availableProcessors(), compressionLevel);
    }

    @Override
    public void flush() throws IOException {
        while (numUncompressedBytes > 0) {
            deflateBlock();
        }
        // pool must be empty then flushing
        pool.flush();
        codec.getOutputStream().flush();
    }

    /**
     * close() must be called in order to flush any remaining buffered bytes.  An unclosed file will likely be
     * defective.
     *
     */
    @Override
    public void close() throws IOException {
        flush();
        pool.close();
        // For debugging...
        // if (numberOfThrottleBacks > 0) {
        //     System.err.println("In BlockCompressedOutputStream, had to throttle back " + numberOfThrottleBacks +
        //                        " times for file " + codec.getOutputFileName());
        // }
        codec.writeBytes(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
        codec.close();
        // Can't re-open something that is not a regular file, e.g. a named pipe or an output stream
        if (this.file == null || !this.file.isFile()) return;
        if (BlockCompressedInputStream.checkTermination(this.file) !=
                BlockCompressedInputStream.FileTermination.HAS_TERMINATOR_BLOCK) {
            throw new IOException("Terminator block not found after closing BGZF file " + this.file);
        }
    }

    /**
     * Override because new parallel indexing algorithm
     * While block is compressing asynchronously it has pointer [blockIdx-1, uncompressedBytes]
     * @see ParallelBAMIndexer
     */
    @Override
    public long getFilePointer(){
        return BlockCompressedFilePointerUtil.makeFilePointer(currentBlockIdx, numUncompressedBytes);
    }

    /**
     * Push deflate order to the first available deflater (wait, if the are no available).
     * Copy {@link #uncompressedBuffer} to internal storage and release {@link #numUncompressedBytes}
     */
    @Override
    protected int deflateBlock() {
        if (numUncompressedBytes == 0) {
            return 0;
        }
        pool.deflateAsyncOnNextAvailable(currentBlockIdx++, uncompressedBuffer, numUncompressedBytes);
        numUncompressedBytes = 0;
        return 0;
    }

    /**
     * With parallel compressing it's required to use ParallelBAMIndexer
     * @see ParallelBAMIndexer
     * @param indexer
     */
    public void setIndexer(final BAMIndexer indexer) {
        if(indexer != null) {
            if (!(indexer instanceof ParallelBAMIndexer)) {
                throw new IllegalArgumentException("Use ParallelBAMIndexer with ParallelBlockCompressedOutputStream");
            }
            this.indexer = (ParallelBAMIndexer) indexer;
        }
    }

    /**
     * Thread safe, sequential writing to the file.
     * Next block will be written only if blockIDX argument equals {@link #nextBlockIdxToWrite}
     * After write updating the index (if exist)
     */
    protected synchronized void writeGzipBlockSequential(int blockIDX, final byte[] compressedBuffer, final int compressedSize, final int uncompressedSize, final long crc) throws InterruptedException {
        while(blockIDX != nextBlockIdxToWrite){
            this.wait();
        }
        mBlockAddress += super.writeGzipBlock(compressedBuffer, compressedSize, uncompressedSize, crc);
        if(indexer!=null){
            indexer.updateAllTempRecords(nextBlockIdxToWrite, mBlockAddress);
        }
        nextBlockIdxToWrite++;
        //nextBlockIdxToWrite increased, so maybe one of sleeping deflaters will accept to the next writing
        this.notifyAll();
    }
}
