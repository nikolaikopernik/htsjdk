package htsjdk.samtools.util;

import htsjdk.samtools.util.zip.DeflaterFactory;

import java.util.BitSet;
import java.util.Queue;
import java.util.zip.CRC32;
import java.util.zip.Deflater;

/**
 * Created by Nikolai_Bogdanov on 11/19/2015.
 */
public class ParallelDeflateWorker extends Thread{
    private final Queue<ParallelDeflateWorker> pool;
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
    private BitSet working = new BitSet(1);
    private final CRC32 crc32 = new CRC32();
    private int numUncompressedBytes;
    private int idx;
    private BlockCompressedOutputStream stream;

    public ParallelDeflateWorker(final Queue<ParallelDeflateWorker> pool, BlockCompressedOutputStream stream, final int compressionLevel) {
        this.pool = pool;
        this.stream = stream;
        working.set(0,false);

        this.deflater = DeflaterFactory.makeDeflater(compressionLevel, true);
        this.noCompressionDeflater = DeflaterFactory.makeDeflater(Deflater.NO_COMPRESSION, true);
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
