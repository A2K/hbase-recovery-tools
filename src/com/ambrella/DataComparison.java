package com.ambrella;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;

import java.io.IOException;
import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class DataComparison {

    public static final String TAG = DataComparison.class.getName();

    final static byte[] DATABLOCKMAGIC = {'D', 'A', 'T', 'A', 'B', 'L', 'K', 42};


    public static void main(String[] args)  {

        if (args.length != 2) {
            log("args: INPUT_FILE OUTPUT_FILE");
            log("Compares data of all keys of two files.");
            System.exit(1);
        }

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", "mtae1,rdaf3,rdaf2,rdaf1,mtae2");

        conf.addResource(new Path("/etc/hadoop-0.20/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop-0.20/conf/hdfs-site.xml"));
        conf.addResource(new Path("/usr/lib/hbase/conf/hbase-site.xml"));
        conf.set("dfs.block.size", "268435456");

        try {
            compareFiles(conf, input, output);
        } catch(Exception e) {
            log("UNEXPECTED EXCEPTION:");
            e.printStackTrace();
            System.exit(2);
        }
    }

    public static void compareFiles(Configuration conf, Path input, Path output) throws IOException, IllegalAccessException, NoSuchFieldException, ClassNotFoundException{

        FileSystem fs = input.getFileSystem(conf);

        Index inputIndex = readIndex(conf, input);
        Index outputIndex = readIndex(conf, output);

        if (inputIndex == null || outputIndex == null) {
            log("ERROR: failed to read index");
            System.exit(2);
        }

        log("read", inputIndex.getLength(), "indexes from input file");
        log("read", outputIndex.getLength(), "indexes from output file");

        if (inputIndex.getLength() != outputIndex.getLength()) {
            log("ERROR: indexes lengths do not match!");
            System.exit(2);
        }

        RangeSet<Long> missingRanges = TreeRangeSet.create();

        FSDataInputStream inputStream = fs.open(input);
        FSDataInputStream outputStream = fs.open(output);


        for (int i = 0; i < inputIndex.getLength(); ++i) {

            log("checking block", i, "of", inputIndex.getLength(), "(" + (i * 100 / inputIndex.getLength()) + "%)");

            if (inputIndex.getSize(i) != outputIndex.getSize(i)) {
                log("ERROR: block size does not match at key index=" + i);
                System.exit(2);
            }

            Range<Long> blockByteRange = Range.closed(inputIndex.getOffset(i), inputIndex.getOffset(i) + inputIndex.getSize(i));

            if (!missingRanges.subRangeSet(blockByteRange).isEmpty()) {
                log("skipping key index=" + i + " because containing block is missing from input file");
                continue;
            }

            Block inputBlock = null;

            try {
                inputStream.seek(inputIndex.getOffset(i));
                inputBlock = readBlock(inputStream, (int) inputIndex.getSize(i));
            } catch (BlockReadError exception) {

                BlockLocation[] locations = fs.getFileBlockLocations(fs.getFileStatus(input),
                        inputIndex.getOffset(i) + exception.getBytesRead(),
                        inputIndex.getSize(i) - exception.getBytesRead());

                for (BlockLocation loc : locations) {
                    Range<Long> range = Range.<Long>closed(loc.getOffset(), loc.getOffset() + loc.getLength());
                    missingRanges.add(range);
                    log("skipping input blocks in non-readable range:", range);
                }

                i -= 1;
                continue;
            }

            Block outputBlock = null;

            try {
                outputStream.seek(outputIndex.getOffset(i));
                outputBlock = readBlock(outputStream, (int) outputIndex.getSize(i));
            } catch (BlockReadError blockReadError) {
                log("ERROR: failed to read output file block:");
                blockReadError.getReadException().printStackTrace();
                System.exit(2);
            }

            if (!Arrays.equals(inputBlock.getMagic(), DATABLOCKMAGIC)) {
                log("input block magic bytes don't match DATABLOCKMAGIC");
                System.exit(2);
            }

            if (!Arrays.equals(outputBlock.getMagic(), DATABLOCKMAGIC)) {
                log("output block magic bytes don't match DATABLOCKMAGIC");
                System.exit(2);
            }

            if (inputBlock.getFirstKeyDataLength() != outputBlock.getFirstKeyDataLength()) {
                log("first key data length does not match in block index=" + i);
                System.exit(2);
            }

            if (!Arrays.equals(inputBlock.getKey(), outputBlock.getKey())) {
                log("ERROR: block keys do not match key index=" + i + ":", inputBlock.getKeyString(), "!=", outputBlock.getKeyString());
                System.exit(2);
            }

            if (inputBlock.getData().length != outputBlock.getData().length) {
                log("blocks data sizes do not match:", inputBlock.getData().length, "!=", outputBlock.getData().length);
                System.exit(2);
            }

            if (!Arrays.equals(inputBlock.getData(), outputBlock.getData())) {
                log("ERROR: block data does not match key index=" + i + ":", inputBlock.getKeyString(), "!=", outputBlock.getKeyString());
                System.exit(2);
            }
        }

        log("VERIFICATION SUCCEEDED");

        System.exit(0);
    }

    static class Block {
        private final byte[] magic;
        private final byte[] key;
        private final int firstKeyDataLength;
        private final byte[] data;

        Block(byte[] magic, byte[] key, int firstKeyDataLength, byte[] data) {
            this.magic = magic;
            this.key = key;
            this.firstKeyDataLength = firstKeyDataLength;
            this.data = data;
        }

        int getFirstKeyDataLength() {
            return firstKeyDataLength;
        }

        byte[] getData() {
            return this.data;
        }

        byte[] getKey() {
            return this.key;
        }

        String getKeyString() {
            return KeyValue.keyToString(this.key);
        }

        byte[] getMagic() {
            return this.magic;
        }
    }

    static class BlockReadError extends Exception {
        private final long bytesRead;
        private final IOException exception;

        public BlockReadError(long bytesRead, IOException exception) {
            this.bytesRead = bytesRead;
            this.exception = exception;
        }

        long getBytesRead() {
            return bytesRead;
        }

        IOException getReadException() {
            return exception;
        }
    }


    private static Block readBlock(FSDataInputStream stream, int blockSize) throws BlockReadError {

        long bytesRead = 0;

        try {
            long pos = stream.getPos();
            byte[] magic = new byte[DATABLOCKMAGIC.length];
            stream.readFully(magic);
            bytesRead += magic.length;
            stream.seek(pos + bytesRead);
            int keyLength = stream.readInt();
            bytesRead += 4;
            stream.seek(pos + bytesRead);
            int dataLength = stream.readInt();
            bytesRead += 4;
            byte[] key = new byte[keyLength];
            stream.seek(pos + bytesRead);
            stream.readFully(key);
            bytesRead += key.length;

            byte[] data = new byte[(int) (blockSize - bytesRead)];
            stream.seek(pos + bytesRead);
            stream.readFully(data);
            bytesRead += data.length;

            return new Block(magic, key, dataLength, data);

        } catch (IOException e) {

            throw new BlockReadError(bytesRead, e);

        }

    }

    private static HFileScanner makeScanner(Path input, Configuration conf, FileSystem fs) throws IOException {
        HFile.Reader inputReader = new HFile.Reader(fs, input, StoreFile.getBlockCache(conf), false);
        inputReader.loadFileInfo();
        HFileScanner scanner = inputReader.getScanner(false, false);
        if (!scanner.seekTo()) {
            log("ERROR: HFileScanner can not seekTo() on", input);
        }
        return scanner;
    }

    static class Index {

        final byte[][] keys;
        final long[] offsets;
        final int[] sizes;

        Index(byte[][] keys, long[] offsets, int[] sizes) {
            this.keys = keys;
            this.offsets = offsets;
            this.sizes = sizes;
        }

        long getLength() {
            return keys.length;
        }

        byte[] getKey(int index) {
            return keys[index];
        }

        long getOffset(int index) {
            return offsets[index];
        }

        long getSize(int index) {
            return sizes[index];
        }

    }

    private static Index readIndex(Configuration conf, Path filepath) throws IllegalAccessException, ClassNotFoundException, NoSuchFieldException, IOException {
        HFile.Reader reader = new HFile.Reader(filepath.getFileSystem(conf), filepath, StoreFile.getBlockCache(conf), false);

        reader.loadFileInfo();

        Field blockIndexField = null;
        try {
            blockIndexField = reader.getClass().getDeclaredField("blockIndex");
        } catch (NoSuchFieldException e) {
            log("Cloud not get HFile$BlockIndex class: " + e.getMessage());
            return null;
        }

        blockIndexField.setAccessible(true);
        Object blockIndex = blockIndexField.get(reader);

        Class<?> c = Class.forName(HFile.class.getName() + "$BlockIndex");

        Field bkeys = c.getDeclaredField("blockKeys");
        bkeys.setAccessible(true);
        byte[][] keys = (byte[][]) bkeys.get(blockIndex);

        Field boffsets = c.getDeclaredField("blockOffsets");
        boffsets.setAccessible(true);
        long[] offsets = (long[]) boffsets.get(blockIndex);

        Field bsizes = c.getDeclaredField("blockDataSizes");
        bsizes.setAccessible(true);
        int[] sizes = (int[]) bsizes.get(blockIndex);

        return new Index(keys, offsets, sizes);
    }

    public static String formatFileSize(long size) {
        if (size <= 0) return "0";
        final String[] units = new String[]{"B", "kB", "MB", "GB", "TB"};
        int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
        return new DecimalFormat("#,##0.#").format(size / Math.pow(1024, digitGroups)) + " " + units[digitGroups];
    }

    static void log(Object... objects) {
        String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").format(new Date());

        System.out.print(time + " ");

        for (Object object : objects) {
            System.out.print(object.toString() + " ");
        }
        System.out.println();
    }
}
