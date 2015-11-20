package htsjdk.samtools.util;

import htsjdk.samtools.Defaults;
import htsjdk.samtools.util.zip.DeflaterFactory;

import java.util.BitSet;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.zip.CRC32;
import java.util.zip.Deflater;

/**
 * Created by Nikolai_Bogdanov on 11/19/2015.
 */
public class ParallelDeflateWorker extends Thread{
    private final Queue<ParallelDeflateWorker> pool;
    private Deflater deflater = null;
    private Deflater noCompressionDeflater = null;
    private byte[] uncompressedBuffer =
            new byte[BlockCompressedStreamConstants.DEFAULT_UNCOMPRESSED_BLOCK_SIZE];
    private byte[] compressedBuffer =
            new byte[BlockCompressedStreamConstants.MAX_COMPRESSED_BLOCK_SIZE -
                    BlockCompressedStreamConstants.BLOCK_HEADER_LENGTH];
    private BitSet working = new BitSet(1);
    private final CRC32 crc32 = new CRC32();
    private int numUncompressedBytes;
    private int idx;
    private BlockCompressedOutputStream stream;

    public ParallelDeflateWorker(final Queue<ParallelDeflateWorker> pool, BlockCompressedOutputStream stream) {
        this.pool = pool;
        this.stream = stream;
        working.set(0,false);
    }

    @Override
    public void run() {
        try {
            while(!isInterrupted()) {
                synchronized (working) {
                    while (working.get(0) != true) {
                        working.wait();
                    }


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

                    stream.writeGzipBlock(idx, compressedBuffer, compressedSize, bytesToCompress, crc32.getValue());


                    working.set(0,false);
                    working.notifyAll();
                    pool.add(this);
                }

            }
        } catch (InterruptedException e) {
            working.set(0,false);
        }

    }

    public void deflateAsynch(int idx, final byte[] uncompressedBuffer, int numUncompressedBytes) {
        if(this.deflater==null){
            //init
            this.deflater = DeflaterFactory.makeDeflater(Defaults.COMPRESSION_LEVEL, true);
            this.noCompressionDeflater = DeflaterFactory.makeDeflater(Deflater.NO_COMPRESSION, true);
        }
        this.idx = idx;
        this.numUncompressedBytes = numUncompressedBytes;
        System.arraycopy(uncompressedBuffer,0,this.uncompressedBuffer,0,numUncompressedBytes);
        pool.remove(this);
        if(!isAlive()){
            start();
        }
//        System.out.println("== start deflating "+idx+" block");
        synchronized (working){
            working.set(0, true);
            working.notifyAll();
        }

    }

    public void waitCurrentWork() {
        try {
           synchronized (working){
               while (working.get(0) == true){
                   working.wait();
               }
           }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
