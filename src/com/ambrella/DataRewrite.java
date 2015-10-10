package com.ambrella;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataRewrite {

    public static final String TAG = DataRewrite.class.getName();
    private static ClientProtocol namenode = null;

    final static byte[] DATABLOCKMAGIC = {'D', 'A', 'T', 'A', 'B', 'L', 'K', 42};

    final byte[] TRAILERBLOCKMAGIC = {'T', 'R', 'A', 'B', 'L', 'K', 34, 36};

    public static List<LocatedBlock> findMissingBlocks(String path, long fileLength, Configuration conf) throws IOException {

        List<LocatedBlock> res = new LinkedList<LocatedBlock>();

        if (namenode == null) {
            namenode = DFSClient.createNamenode(conf);
        }

        LocatedBlocks locatedBlocks = namenode.getBlockLocations(path, 0, fileLength);
        List<LocatedBlock> blocks = locatedBlocks.getLocatedBlocks();

        for (LocatedBlock block : blocks) {

            if (block.isCorrupt()) {
                res.add(block);
                continue;
            }

            DatanodeInfo[] locs = block.getLocations();

            if (locs.length == 0) {
                res.add(block);
            }
        }

        return res;
    }

    public static void main(String[] args) throws IOException, IllegalAccessException, ClassNotFoundException, NoSuchFieldException {

        if (args.length != 3) {
            log("Arguments:");
            log("INPUT_FILE OUTPUT_FILE MISSING_BLOCKS_LOGFILE");
            log("Input and output files must be in HDFS, missing blocks log can be only local file.");
            System.exit(1);
        }

        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", "mtae1,rdaf3,rdaf2,rdaf1,mtae2");

        conf.addResource(new Path("/etc/hadoop-0.20/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop-0.20/conf/hdfs-site.xml"));
        conf.addResource(new Path("/usr/lib/hbase/conf/hbase-site.xml"));
        conf.set("dfs.block.size", "268435456");

        Path file = new Path(args[0]);

        Path output = new Path(args[1]);
        log("Reading: " + file + ", writing: " + output);

        String missingLogName = args[2];
        File missingLogCheckFile = new File(missingLogName);

        if (missingLogCheckFile.exists()) {
            log("Warning: missing blocks log file already exists and will be overwritten.");
        }

        File missingLogDir = missingLogCheckFile.getAbsoluteFile().getParentFile();
        if (!missingLogDir.exists()) {
            log("Missing blocks log directory does not exist, creating it: " + missingLogDir);
            if (!missingLogDir.mkdirs()) {
                log("Failed to create missing blocks log file directory, can not continue: " + missingLogDir);
                System.exit(2);
            }
        }

        FileOutputStream missingLog = new FileOutputStream(missingLogName, false);

        FileSystem fs = file.getFileSystem(conf);

        if (!fs.exists(file)) {
            log("ERROR, file does not exist: " + file);
            return;
        }

        HFile.Reader reader = new HFile.Reader(fs, file, null, false);

        Map<byte[], byte[]> fileInfo = reader.loadFileInfo();

        long totalSize = fs.getFileStatus(file).getLen();

        log("Input file size:", formatFileSize(totalSize));

        RangeSet<Long> missingRanges = TreeRangeSet.create();

        Field blockIndexField = null;
        try {
            blockIndexField = reader.getClass().getDeclaredField("blockIndex");
        } catch (NoSuchFieldException e) {
            log("Cloud not get HFile$BlockIndex class: " + e.getMessage());
            return;
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


        log("Starting to process total " + totalSize + " bytes");
        FSDataInputStream inputStream = fs.open(file);
        FSDataOutputStream outputStream = fs.create(output);

        int lostKeysCount = 0;

        for (int i = 0; i < keys.length; ++i) {
            byte[] key = keys[i];
            int size = sizes[i];
            size -= key.length + 4 + 4 + DATABLOCKMAGIC.length;

            inputStream.seek(offsets[i]);

            if (!missingRanges.subRangeSet(Range.closed(offsets[i], offsets[i] + sizes[i])).isEmpty()) {

                log("MISSING: key=" + KeyValue.keyToString(key) + ", offset=" + offsets[i] + ", size=" + sizes[i]);

                lostKeysCount += 1;

                missingLog.write(KeyValue.keyToString(key).getBytes());
                missingLog.write("\n".getBytes());

                outputStream.write(DATABLOCKMAGIC);
                outputStream.writeInt(key.length);
                outputStream.writeInt(size);
                outputStream.write(key);
                outputStream.write(new byte[size]);

            } else {

                byte[] buffer = new byte[size];
                int keySize = -1;
                long processedBytes = 0;
                int firstRecordDataSize;

                try {
                    inputStream.skipBytes(DATABLOCKMAGIC.length);
                    processedBytes += DATABLOCKMAGIC.length;
                    keySize = inputStream.readInt();
                    processedBytes += 4;
                    firstRecordDataSize = inputStream.readInt();
                    processedBytes += 4;
                    inputStream.skipBytes(keySize);
                    processedBytes += keySize;
                    long readByteCount = inputStream.read(buffer, 0, size);
                    processedBytes += readByteCount;
                    while (readByteCount < size) {
                        long read = inputStream.read(buffer, (int) readByteCount, (int) (size - readByteCount));
                        readByteCount += read;
                        processedBytes += read;
                    }

                } catch (IOException e) {

                    BlockLocation[] locations = fs.getFileBlockLocations(fs.getFileStatus(file), offsets[i] + processedBytes, sizes[i] - processedBytes);

                    for (BlockLocation loc : locations) {
                        Range<Long> range = Range.<Long>closed(loc.getOffset(), loc.getOffset() + loc.getLength());
                        log("adding range to missing:", range);
                        missingRanges.add(range);
                    }

                    i -= 1;
                    continue;
                }

                outputStream.write(DATABLOCKMAGIC);
                outputStream.writeInt(key.length);
                outputStream.writeInt(firstRecordDataSize);
                outputStream.write(key);
                outputStream.write(buffer);

            }

            double progress = ((((double) Math.round(((double) outputStream.getPos()) * 10000.0 / totalSize))) / 100.0);
            String currentSize = String.format("%0" + ("" + totalSize).length() + "d", outputStream.getPos());
            log("progress: " + (currentSize + "/" + totalSize + " bytes, ") + String.format("%.02f", progress) + "%");

        }

        missingLog.close();

        log("Copying file trailer");

        inputStream.seek(offsets[offsets.length - 1] + sizes[sizes.length - 1]);

        while (inputStream.getPos() < totalSize) {

            byte[] buffer = new byte[102400];
            int bytesRead = inputStream.read(buffer, 0, buffer.length);

            outputStream.write(buffer, 0, bytesRead);
        }

        double progress = ((((double) Math.round(((double) outputStream.getPos()) * 10000.0 / totalSize))) / 100.0);
        String currentSize = String.format("%0" + ("" + totalSize).length() + "d", outputStream.getPos());
        log("progress: " + (currentSize + "/" + totalSize + " bytes, ") + String.format("%.02f", progress) + "%");

        inputStream.close();
        outputStream.sync();
        outputStream.close();

        log("total number of lost keys:", lostKeysCount, "(" + (((double) lostKeysCount) / ((double) keys.length) * 100) + "%)");
        log("file with list of lost keys:", new File(missingLogName).getAbsolutePath());

        log("done, verifying the file");

        log("Input and output files sizes match:", fs.getLength(file) == fs.getLength(output));

        System.exit(0);

        /*
        if (verify(conf, fs, file, output, keys.length, missingRanges)) {

            System.exit(0);

            if (verifyChecksums(conf, fs, file, output, missingRanges)) {
                System.exit(0);
            } else {
                System.exit(2);
            }
        } else {
            System.exit(2);
        }
        */
    }

    static boolean verify(Configuration conf, FileSystem fs, Path input, Path output, int keyCount, RangeSet<Long> missingRanges) {
        try {

            HFile.Reader reader = new HFile.Reader(fs, output, StoreFile.getBlockCache(conf), false);
            reader.loadFileInfo();

            HFileScanner scanner = reader.getScanner(false, false);
            if (!scanner.seekTo()) {
                log("VERIFICATION FAILED: HFileScanner can not seekTo() on created file.");
                return false;
            }

            long processedKeysCount = 0;
            do {
                KeyValue kv = scanner.getKeyValue();
                processedKeysCount += 1;
                log("verified key", processedKeysCount, "out of", keyCount, "(" + (processedKeysCount * 100 / keyCount) + "%)");

            } while (scanner.next());

            log("VERIFICATION SUCCEEDED.");
            return true;

        } catch (Exception e) {

            log("VERIFICATION FAILED:");
            e.printStackTrace();
            return false;

        }
    }

    static boolean verifyChecksums(Configuration conf, FileSystem fs, Path input, Path output, RangeSet<Long> missingRanges) throws IOException {
        FSDataInputStream inputStream = fs.open(input);
        FSDataInputStream outputStream = fs.open(output);

        final int BLOCK_SIZE = 256 * 1024 * 1024;

        long totalLength = fs.getFileStatus(input).getLen();

        final byte[] inputBuffer = new byte[BLOCK_SIZE];
        final byte[] outputBuffer = new byte[BLOCK_SIZE];

        log("starting files content comparison");

        for(long position = 0; position < totalLength; position += BLOCK_SIZE) {
            long inputBytesRead = 0;
            long outputBytesRead = 0;

            try {
                inputStream.seek(position);
                while (inputStream.getPos() < totalLength && inputBytesRead != inputBuffer.length) {
                    inputBytesRead += inputStream.read(inputBuffer, (int)inputBytesRead, (int)(inputBuffer.length - inputBytesRead));
                }
            } catch(IOException e) {
                log("failed to read input file block: " + e);
                log("skipping block check because input file block is missing");
                continue;
            }

            try {
                outputStream.seek(position);
                while (outputStream.getPos() < totalLength && outputBytesRead != outputBuffer.length) {
                    outputBytesRead += outputStream.read(outputBuffer, (int)outputBytesRead, (int)(outputBuffer.length - outputBytesRead));
                }
            } catch(IOException e) {
                log("failed to read output file block: " + e);
                return false;
            }


            if (!Arrays.equals(inputBuffer, outputBuffer)) {
                log("Block content does not match at position", position);
                return false;
            }

        }

        log("finished comparing files (all bytes match)");
        return true;
    }

    public static String formatFileSize(long size) {
        if(size <= 0) return "0";
        final String[] units = new String[] { "B", "kB", "MB", "GB", "TB" };
        int digitGroups = (int) (Math.log10(size)/Math.log10(1024));
        return new DecimalFormat("#,##0.#").format(size/Math.pow(1024, digitGroups)) + " " + units[digitGroups];
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
