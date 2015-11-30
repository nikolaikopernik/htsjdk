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
package htsjdk.samtools;

import htsjdk.samtools.util.BlockCompressedFilePointerUtil;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.testng.Assert.*;

/**
 * Unit test for parallel indexer
 *
 * @author Nikolai_Bogdanov@epam.com
 */
public class ParallelBAMIndexerTest {

    @Test
    public void shouldProcessFirstBlockWithoutChanges() throws IOException{
        final int[] recordWritted = new int[1];
        ParallelBAMIndexer indexer = new ParallelBAMIndexer(File.createTempFile("parallel_index_file.",".bai"), new SAMFileHeader()){
            @Override
            protected void processRecord(final SAMRecord record) {
                assertRecordInRange(record, 0, 0);
                recordWritted[0]++;
            }
        };

        indexer.processAlignment(create(0,13456, 0,14567));
        indexer.processAlignment(create(0,14567, 0,19400));
        indexer.processAlignment(create(0,19400, 0,21850));
        indexer.processAlignment(create(0,21850, 0,24900));
        indexer.processAlignment(create(0,24900, 0,25805));

        assertEquals(indexer.recordsInWait.size(), 5);

        indexer.updateAllTempRecords(0, 18900);
        assertEquals(indexer.recordsInWait.size(), 0);
        assertEquals(recordWritted[0], 5);
        indexer.finish();
    }

    @Test
    public void shouldConvertBlockIndexToAddress() throws IOException{
        final int[] recordExpect = new int[2];//0-counter,1-blockAddressToCompare
        ParallelBAMIndexer indexer = new ParallelBAMIndexer(File.createTempFile("parallel_index_file.",".bai"), new SAMFileHeader()){
            @Override
            protected void processRecord(final SAMRecord record) {
                assertRecordInRange(record, recordExpect[1], recordExpect[1]);
                recordExpect[0]++;
            }
        };

        indexer.processAlignment(create(1,13456, 1,14567));
        indexer.processAlignment(create(2,14567, 2,19400));
        indexer.processAlignment(create(3,19400, 3,21850));

        assertEquals(indexer.recordsInWait.size(), 3);

        recordExpect[1] = 1111;
        indexer.updateAllTempRecords(0, 1111);
        assertEquals(indexer.recordsInWait.size(), 2);

        recordExpect[1] = 2222;
        indexer.updateAllTempRecords(1, 2222);
        assertEquals(indexer.recordsInWait.size(), 1);

        recordExpect[1] = 3333;
        indexer.updateAllTempRecords(2, 3333);
        assertEquals(indexer.recordsInWait.size(), 0);

        indexer.updateAllTempRecords(3, 4444);
        assertEquals(recordExpect[0], 3);
        indexer.finish();
    }

    @Test
    public void shouldConvertBoundaryRecords() throws IOException{
        final int[] recordExpect = new int[2];//0-counter,1-blockAddressToCompare
        ParallelBAMIndexer indexer = new ParallelBAMIndexer(File.createTempFile("parallel_index_file.",".bai"), new SAMFileHeader()){
            @Override
            protected void processRecord(final SAMRecord record) {
                assertRecordInRange(record, 0, recordExpect[1]);
                recordExpect[0]++;
            }
        };

        indexer.processAlignment(create(0, 13456, 1, 14567));
        indexer.processAlignment(create(1, 14567, 2, 19400));
        indexer.processAlignment(create(2, 19400, 3, 21850));

        assertEquals(indexer.recordsInWait.size(), 3);

        recordExpect[1] = 1111;
        indexer.updateAllTempRecords(0, 1111);
        assertEquals(indexer.recordsInWait.size(), 2);

        recordExpect[1] = 2222;
        indexer.updateAllTempRecords(1, 2222);
        assertEquals(indexer.recordsInWait.size(), 1);

        recordExpect[1] = 3333;
        indexer.updateAllTempRecords(2, 3333);
        assertEquals(indexer.recordsInWait.size(), 0);

        indexer.updateAllTempRecords(3, 4444);
        assertEquals(recordExpect[0], 3);
        indexer.finish();
    }


    public static SAMRecord create(int fromBlock, int fromNum, int toBlock, int toNum){
        SAMRecord record = new SAMRecord(new SAMFileHeader());
        record.setFileSource(new SAMFileSource(null, new BAMFileSpan(
                new Chunk(BlockCompressedFilePointerUtil.makeFilePointer(fromBlock, fromNum),
                        BlockCompressedFilePointerUtil.makeFilePointer(toBlock,toNum)))));
        return record;
    }

    public static void assertRecordInRange(final SAMRecord record, final int fromBlockIdx, final int toBlockIdx) {
        BAMFileSpan span = (BAMFileSpan) record.getFileSource().getFilePointer();
        List<Chunk> chunks = span.getChunks();
        for(Chunk c:chunks){
            if(BlockCompressedFilePointerUtil.getBlockAddress(c.getChunkStart()) < fromBlockIdx ||
                    BlockCompressedFilePointerUtil.getBlockAddress(c.getChunkEnd()) > toBlockIdx) {
                fail("SAMRecord " + c + " in out of test range [" + fromBlockIdx + ";" + toBlockIdx + "]");
            }
        }
    }

}