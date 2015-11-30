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

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Parallel indexer for asynchronous deflating BAM block ({@link htsjdk.samtools.util.ParallelBlockCompressedOutputStream})
 * Uses only in save indexes. To read index file use BAMIndexer
 *
 * @author Nikolai_Bogdanov@epam.com
 */
public class ParallelBAMIndexer extends BAMIndexer {
    protected List<SAMRecord> recordsInWait = new ArrayList<>(500);

    public ParallelBAMIndexer(final File output, final SAMFileHeader fileHeader) {
        super(output, fileHeader);
    }

    @Override
    public void processAlignment(final SAMRecord rec) {
        synchronized (recordsInWait){
            recordsInWait.add(rec);
        }
    }

    @Override
    public void finish() {
        synchronized (recordsInWait){
            try{
                while(!recordsInWait.isEmpty()){
                    recordsInWait.wait();
                }
            }catch (InterruptedException e){}
        }
        super.finish();
    }

    /**
     * Block with sequential number {@code blockIDX} has been processed to address = {@code blockAddress}
     * So now we can update indexes for next block (index = {@code blockIDX} +1)
     *
     * @param blockIDX block index
     * @param blockAddress block address
     */
    public synchronized void updateAllTempRecords(int blockIDX, long blockAddress){
        List<SAMRecord> records = new ArrayList<>(500);
        synchronized (recordsInWait){
            for(Iterator<SAMRecord> i = recordsInWait.iterator();i.hasNext();){
                SAMRecord record = i.next();
                if(updateRecord(record, blockIDX + 1, blockAddress)) {
                    records.add(record);
                    i.remove();
                }else {
                    break ;
                }
            }
        }

        for(SAMRecord record:records){
            processRecord(record);
        }

        if(!records.isEmpty()) {
            //notify some work finished (for correct finish() method)
            synchronized (recordsInWait) {
                recordsInWait.notifyAll();
            }
        }
    }

    /**
     * For test overriding
     * Add mockito to the project to disable that test methods!
     * @param record
     */
    protected void processRecord(final SAMRecord record) {
        super.processAlignment(record);
    }

    protected boolean updateRecord(SAMRecord record, int blockIdx, long blockAddress){
        BAMFileSpan span = (BAMFileSpan) record.getFileSource().getFilePointer();
        List<Chunk> chunks = span.getChunks();
        long start = chunks.get(0).getChunkStart();
        long end = chunks.get(0).getChunkEnd();
        if(BlockCompressedFilePointerUtil.getBlockAddress(start) > blockIdx &&
                BlockCompressedFilePointerUtil.getBlockAddress(end) > blockIdx){
            return false;
        }
        for(Chunk c:chunks){
            long blockStart = BlockCompressedFilePointerUtil.getBlockAddress(c.getChunkStart());
            long blockEnd = BlockCompressedFilePointerUtil.getBlockAddress(c.getChunkEnd());
            if(blockStart == blockIdx){
                c.setChunkStart(BlockCompressedFilePointerUtil.makeFilePointer(blockAddress, BlockCompressedFilePointerUtil.getBlockOffset(c.getChunkStart())));
            }
            if(blockEnd == blockIdx) {
                c.setChunkEnd(BlockCompressedFilePointerUtil.makeFilePointer(blockAddress, BlockCompressedFilePointerUtil.getBlockOffset(c.getChunkEnd())));
            }else if(blockEnd != 0){ //for the first block blockIdx == blockAddress == 0
                return false;
            }
        }
        return true;
    }
}
