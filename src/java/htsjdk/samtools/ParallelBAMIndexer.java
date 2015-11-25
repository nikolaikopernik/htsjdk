package htsjdk.samtools;

import htsjdk.samtools.util.BlockCompressedFilePointerUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Parallel indexer for asynchronous deflating BAM block ({@link htsjdk.samtools.util.ParallelBlockCompressedOutputStream})
 * Uses only in save indexes. To read index file use BAMIndexer
 */
public class ParallelBAMIndexer extends BAMIndexer {
    private List<SAMRecord> recordsInWait = new ArrayList<>(500);

    public ParallelBAMIndexer(final File output, final SAMFileHeader fileHeader) {
        super(output, fileHeader);
    }
    public long prev = 0;
    public long count = 0;

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
     * Next block has been compressed and saved - so now we have it compressed address and can update all
     * temporary indexes
     * @param blockIDX
     * @param blockAddress
     */
    public synchronized void indexAllTempRecords(int blockIDX, long blockAddress){
        List<SAMRecord> records = new ArrayList<>(500);
        synchronized (recordsInWait){
            for(Iterator<SAMRecord> i = recordsInWait.iterator();i.hasNext();){
                SAMRecord record = i.next();
                if(updateRecord(record, blockIDX, blockAddress)) {
                    records.add(record);
                    i.remove();
                }else {
                    break ;
                }
            }
        }

        for(SAMRecord record:records){
            super.processAlignment(record);
        }

        if(!records.isEmpty()) {
            //notify some work finished (for correct finish() method)
            synchronized (recordsInWait) {
                recordsInWait.notifyAll();
            }
        }
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
            if(BlockCompressedFilePointerUtil.getBlockAddress(c.getChunkStart()) == blockIdx){
                c.setChunkStart(BlockCompressedFilePointerUtil.makeFilePointer(blockAddress, BlockCompressedFilePointerUtil.getBlockOffset(c.getChunkStart())));
            }
            if(BlockCompressedFilePointerUtil.getBlockAddress(c.getChunkEnd()) == blockIdx) {
                c.setChunkEnd(BlockCompressedFilePointerUtil.makeFilePointer(blockAddress, BlockCompressedFilePointerUtil.getBlockOffset(c.getChunkEnd())));
            }else {
                return false;
            }
        }
        return true;
    }
}
