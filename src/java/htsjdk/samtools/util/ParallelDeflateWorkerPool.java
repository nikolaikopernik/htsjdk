package htsjdk.samtools.util;

import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


/**
 * Created by Nikolai_Bogdanov on 11/19/2015.
 */
public class ParallelDeflateWorkerPool {
    private BlockingQueue<ParallelDeflateWorker> pool;
    private ArrayList<ParallelDeflateWorker> array = new ArrayList<>(10);
    private int blockOrder = 0;


    public ParallelDeflateWorkerPool(BlockCompressedOutputStream stream, final int processCount) {
        pool = new ArrayBlockingQueue<>(processCount);
        for(int i = 0;i<processCount;i++){
            ParallelDeflateWorker e = new ParallelDeflateWorker(pool, stream);
            pool.add(e);
            array.add(e);
        }
    }


    public void deflateAsynchOnNextAvailable(final byte[] uncompressedBuffer, int numUncompressedBytes) {
        try{
            pool.take().deflateAsynch(blockOrder++, uncompressedBuffer, numUncompressedBytes);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    public void close(){
        for(ParallelDeflateWorker w:array){
            w.interrupt();

            try {
                w.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void flush() {
        for(ParallelDeflateWorker w:array){
            w.waitCurrentWork();
        }
    }
}
