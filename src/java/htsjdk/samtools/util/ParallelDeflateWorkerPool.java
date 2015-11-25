package htsjdk.samtools.util;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


/**
 * Created by Nikolai_Bogdanov on 11/19/2015.
 */
public class ParallelDeflateWorkerPool {
    private BlockingQueue<ParallelDeflateWorker> pool;
    private ArrayList<ParallelDeflateWorker> array = new ArrayList<>(10);


    public ParallelDeflateWorkerPool(ParallelBlockCompressedOutputStream stream, final int processCount, final int compressionLevel) {
        pool = new ArrayBlockingQueue<>(processCount);
        for(int i = 0;i<processCount;i++){
            ParallelDeflateWorker e = new ParallelDeflateWorker(pool, stream, compressionLevel);
            pool.add(e);
            array.add(e);
        }
    }


    public void deflateAsynchOnNextAvailable(int idx, final byte[] uncompressedBuffer, int numUncompressedBytes) {
        try{
            pool.take().deflateAsynch(idx, uncompressedBuffer, numUncompressedBytes);
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
