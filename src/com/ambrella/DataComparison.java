package com.ambrella;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;
import java.util.Arrays;

import static com.ambrella.Log.log;
import static com.ambrella.Utils.readHFileIndex;

public class DataComparison {

    public static void main(String[] args) {

        if (args.length != 2) {
            log("args: INPUT_FILE OUTPUT_FILE");
            log("Compares data of all keys of two files.");
            System.exit(1);
        }

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Configuration conf = Config.Hadoop.makeConfig();

        try {
            compareFiles(conf, input, output);
        } catch (Exception e) {
            log("UNEXPECTED EXCEPTION:");
            e.printStackTrace();
            System.exit(2);
        }
    }

    public static void compareFiles(Configuration conf, Path input, Path output) throws IOException, IllegalAccessException, NoSuchFieldException, ClassNotFoundException {

        FileSystem fs = input.getFileSystem(conf);

        Utils.Index inputIndex = readHFileIndex(conf, input);
        Utils.Index outputIndex = readHFileIndex(conf, output);

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

            HFileDataBlock inputDataBlock = HFileDataBlock.read(inputStream, (int) inputIndex.getSize(i));
            HFileDataBlock outputDataBlock = HFileDataBlock.read(outputStream, (int) outputIndex.getSize(i));

            if (inputDataBlock.getRecords().size() != outputDataBlock.getRecords().size()) {
                log("ERROR: record count does not match in data block index=" + i);
                System.exit(2);
            }

            inputStream.seek(inputIndex.getOffset(i));
            outputStream.seek(outputIndex.getOffset(i));

            for (int j = 0; j < inputDataBlock.getRecords().size(); ++j) {
                HFileDataBlockRecord inputBlock;
                HFileDataBlockRecord outputBlock;
                try {
                    inputBlock = inputDataBlock.getRecords().get(j);
                    outputBlock = outputDataBlock.getRecords().get(j);
                } catch (IOException exception) {

                    BlockLocation[] locations = fs.getFileBlockLocations(fs.getFileStatus(input),
                            inputIndex.getOffset(i),
                            inputIndex.getSize(i));

                    for (BlockLocation loc : locations) {
                        Range<Long> range = Range.<Long>closed(loc.getOffset(), loc.getOffset() + loc.getLength());
                        missingRanges.add(range);
                        log("skipping input blocks in non-readable range:", range);
                    }

                    i -= 1;
                    continue;
                }

                if (!Arrays.equals(inputBlock.key, outputBlock.key)) {
                    log("ERROR: block keys do not match key index=" + i + ":", KeyValue.keyToString(inputBlock.key), "!=", KeyValue.keyToString(outputBlock.key));
                    System.exit(2);
                }

                if (inputBlock.data.length != outputBlock.data.length) {
                    log("blocks data sizes do not match:", inputBlock.data.length, "!=", outputBlock.data.length);
                    System.exit(2);
                }

                if (!Arrays.equals(inputBlock.data, outputBlock.data)) {
                    log("ERROR: block data does not match key index=" + i);
                    System.exit(2);
                }
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
            byte[] magic = new byte[C.DATABLOCKMAGIC.length];
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

}
