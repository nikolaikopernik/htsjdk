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

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;


/**
 * Pool of parallel compress workers
 *
 * @author Nikolai_Bogdanov@epam.com
 */
public class ParallelDeflatersPool {
    // Pool of available deflaters
    private ArrayBlockingQueue<ParallelDeflater> pool;
    // All created deflaters
    private ArrayList<ParallelDeflater> array;

    /**
     * Create new pool and fill it.
     * Deflaters are still stopped.
     * @param stream stream to final writing the compressed data
     * @param processCount count of parallel deflaters
     * @param compressionLevel compression level
     */
    public ParallelDeflatersPool(ParallelBlockCompressedOutputStream stream, final int processCount, final int compressionLevel) {
        pool = new ArrayBlockingQueue<>(10);
        array = new ArrayList<>(processCount);
        for(int i = 0; i<processCount; i++){
            ParallelDeflater e = new ParallelDeflater(this, stream, compressionLevel);
            pool.add(e);
            array.add(e);
        }
    }

    /**
     * Take first available deflater and give a job
     * @param idx sequential block index
     * @param uncompressedBuffer data to deflate in byte buffer
     * @param numUncompressedBytes length of uncompressed data
     */
    public void deflateAsyncOnNextAvailable(int idx, final byte[] uncompressedBuffer, int numUncompressedBytes) {
        try{
            pool.take().deflateAsync(idx, uncompressedBuffer, numUncompressedBytes);
        }catch (InterruptedException e){}
    }

    /**
     * Close the pool.
     * Interrupt all deflaters and wait until they stops.
     */
    public void close(){
        for(ParallelDeflater w:array){
            w.interrupt();
            try {
                w.join();
            } catch (InterruptedException e) {}
        }
    }

    /**
     * Wait until all deflaters process their current work
     */
    public void flush() {
        for(ParallelDeflater w:array){
            try {
                w.waitCurrentWork();
            } catch (InterruptedException e) {}
        }
    }

    protected void deflaterBusy(final ParallelDeflater deflater) {
        pool.remove(deflater);
    }

    protected void freeDeflater(ParallelDeflater deflater){
        pool.add(deflater);
    }
}
