/*
 * The MIT License
 *
 * Copyright (c) 2010 The Broad Institute
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

import htsjdk.samtools.util.zip.DeflaterFactory;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;
import java.util.zip.Deflater;

/**
 * Compress worker in separate thread.
 *
 * @author Nikolai_Bogdanov@epam.com
 */
public class ParallelDeflater extends Thread{
    private final ParallelDeflatersPool pool;
    private Deflater deflater;

    // A second deflater is created for the very unlikely case where the regular deflation actually makes
    // things bigger, and the compressed block is too big.  It should be possible to downshift the
    // primary deflater to NO_COMPRESSION level, recompress, and then restore it to its original setting,
    // but in practice that doesn't work.
    // The motivation for deflating at NO_COMPRESSION level is that it will predictably produce compressed
    // output that is 10 bytes larger than the input, and the threshold at which a block is generated is such that
    // the size of tbe final gzip block will always be <= 64K.  This is preferred over the previous method,
    // which would attempt to compress up to 64K bytes, and if the resulting compressed block was too large,
    // try compressing fewer input bytes (aka "downshifting').  The problem with downshifting is that
    // getFilePointer might return an inaccurate value.
    // I assume (AW 29-Oct-2013) that there is no value in using hardware-assisted deflater for no-compression mode,
    // so just use JDK standard.
    private Deflater noCompressionDeflater = null;
    private byte[] uncompressedBuffer =
            new byte[BlockCompressedStreamConstants.DEFAULT_UNCOMPRESSED_BLOCK_SIZE];
    private byte[] compressedBuffer =
            new byte[BlockCompressedStreamConstants.MAX_COMPRESSED_BLOCK_SIZE -
                    BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH];
    private ReentrantLock lock = new ReentrantLock();
    private Condition hasWork = lock.newCondition();

    private final CRC32 crc32 = new CRC32();
    private int numUncompressedBytes;
    private int blockIDX;
    private ParallelBlockCompressedOutputStream stream;

    public ParallelDeflater(final ParallelDeflatersPool pool, ParallelBlockCompressedOutputStream stream, final int compressionLevel) {
        this.pool = pool;
        this.stream = stream;

        this.deflater = DeflaterFactory.makeDeflater(compressionLevel, true);
        this.noCompressionDeflater = DeflaterFactory.makeDeflater(Deflater.NO_COMPRESSION, true);
    }

    @Override
    public void run() {
        try {
            while (!isInterrupted()) {
                waitNextBlock();

                final int bytesToCompress = numUncompressedBytes;
                // Compress the input
                deflater.reset();
                deflater.setInput(uncompressedBuffer, 0, bytesToCompress);
                deflater.finish();
                int compressedSize = deflater.deflate(compressedBuffer, 0, compressedBuffer.length);

                // If it didn't all fit in compressedBuffer.length, set compression level to NO_COMPRESSION
                // and try again.  This should always fit.
                if (!deflater.finished()) {
                    noCompressionDeflater.reset();
                    noCompressionDeflater.setInput(uncompressedBuffer, 0, bytesToCompress);
                    noCompressionDeflater.finish();
                    compressedSize = noCompressionDeflater.deflate(compressedBuffer, 0, compressedBuffer.length);
                    if (!noCompressionDeflater.finished()) {
                        throw new IllegalStateException("unpossible");
                    }
                }

                // Data compressed small enough, so write it out.
                crc32.reset();
                crc32.update(uncompressedBuffer, 0, bytesToCompress);

                stream.writeGzipBlockSequential(blockIDX, compressedBuffer, compressedSize, bytesToCompress, crc32.getValue());
                blockDone();
            }

        }catch (InterruptedException ie){
            //just end run() as result
        }
    }

    private void blockDone() {
        lock.lock();
        try {
            blockIDX = -1;
            pool.freeDeflater(this);
            hasWork.signalAll();
        }finally {
            lock.unlock();
        }
    }

    private void waitNextBlock() throws InterruptedException {
        lock.lock();
        try{
            while (blockIDX == -1){
                hasWork.await();
            }
        }finally {
            lock.unlock();
        }
    }



    /**
     * New work for the deflater.
     * 1. copy all input data
     * 2. test if thread running
     * 3. remove self from available deflaters
     * Not thread-safe method.
     *
     * @param blockIDX
     * @param uncompressedBuffer
     * @param numUncompressedBytes
     */
    public void deflateAsync(int blockIDX, final byte[] uncompressedBuffer, int numUncompressedBytes) {
        this.numUncompressedBytes = numUncompressedBytes;
        System.arraycopy(uncompressedBuffer,0,this.uncompressedBuffer,0,numUncompressedBytes);

        newBlockToDeflate(blockIDX);
    }

    private void newBlockToDeflate(final int blockIDX) {
        if(blockIDX == -1){
            throw new IllegalStateException("Block index cannot be less then zero");
        }
        this.blockIDX = blockIDX;
        pool.deflaterBusy(this);
        if(!isAlive()){
            start();
        }
        lock.lock();
        try{
            hasWork.signalAll();
        }finally {
            lock.unlock();
        }
    }

    public void waitCurrentWork() throws InterruptedException{
        lock.lock();
        try {
           while(blockIDX != -1){
                hasWork.await();
           }
        }finally {
            lock.unlock();
        }
    }
}
