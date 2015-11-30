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
import htsjdk.samtools.SAMFileHeader;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.testng.Assert.*;

/**
 * Unit tests for {@link ParallelBlockCompressedOutputStream}
 * @author Nikolai_Bogdanov@epam.com
 */
public class ParallelBlockCompressedOutputStreamTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldErrorWhileSettingWrongIndexator() throws IOException{
        ParallelBlockCompressedOutputStream stream = new ParallelBlockCompressedOutputStream(new ByteArrayOutputStream(500), null);
        stream.setIndexer(new BAMIndexer(new ByteArrayOutputStream(1000), new SAMFileHeader()));
    }

    @Test
    public void shouldReturnFilePointerBasedOnBlockIdx() throws IOException{
        ParallelBlockCompressedOutputStream stream = new ParallelBlockCompressedOutputStream(new ByteArrayOutputStream(500), null);
        stream.currentBlockIdx = 14;
        stream.numUncompressedBytes = 1456;
        assertEquals(stream.getFilePointer(), BlockCompressedFilePointerUtil.makeFilePointer(14, 1456));
        stream.close();
    }

    @Test
    public void shouldWriteCompressedInfoSequential() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(1000);
        final ParallelBlockCompressedOutputStream stream = new ParallelBlockCompressedOutputStream(bytes, null);

        // create and fill 3 blocks
        int size = 3;
        final byte[][] blocks = new byte[size][100];
        for (int i = 0; i < size; i++) {
            Arrays.fill(blocks[i], (byte) i);
        }

        // write them inversed from 2 to 0
        Thread[] threads = new Thread[size];
        for (int i = size-1; i >=0; i--) {
            final int fi = i;
            threads[i] = writeBlock(new Runnable() {
                @Override
                public void run() {
                    try {
                        stream.writeGzipBlockSequential(fi, blocks[fi], 100, 1345, 3456L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        //wait all threads
        for (int i = 0; i < size; i++) {
            threads[i].join();
        }

        //check that blocks writed sequentially from 0 to 2
        byte[] actual = bytes.toByteArray();
        int blockOffset = 0;
        for (int i = 0; i< size; i++) {
            blockOffset += 18; //head of a block
            assertEquals(Arrays.copyOfRange(actual, blockOffset, blockOffset+100), blocks[i]);
            blockOffset += 100;
            blockOffset += 8; // footer
        }

    }

    public Thread writeBlock(Runnable r){
        Thread t = new Thread(r);
                t.start();
        return t;
    }

}