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
import static com.ambrella.Utils.exit;
import static com.ambrella.Utils.readHFileIndex;

public class BlocksFirstKeyReader {

    public static void main(String[] args) {

        if (args.length != 1) {
            log("args: FILENAME");
            System.exit(1);
        }

        Path file = new Path(args[0]);

        Configuration conf = Config.Hadoop.makeConfig();

        try {
            readKeys(conf, file);
        } catch (Exception e) {
            log("UNEXPECTED EXCEPTION:");
            e.printStackTrace();
            System.exit(2);
        }

        System.exit(0);
    }

    static void readKeys(Configuration conf, Path file) throws IOException, IllegalAccessException, NoSuchFieldException, ClassNotFoundException {

        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream stream = fs.open(file);

        Utils.Index index = readHFileIndex(conf, file);

        RangeSet<Long> missingRanges = TreeRangeSet.create();

        for (int i = 0; i < index.getLength(); i++) {

            long offset = index.getOffset(i);
            long blockSize = index.getSize(i);

            Range<Long> range = Range.<Long>closed(offset, offset + blockSize);
            if (!missingRanges.subRangeSet(range).isEmpty()) {
                log("skipping input block in non-readable range:", range);
                continue;
            }

            byte[] key;

            try {
                key = readBlockKey(stream, offset);
            } catch (IOException e) {
                BlockLocation[] locations = fs.getFileBlockLocations(fs.getFileStatus(file), offset, offset + blockSize);
                for (BlockLocation loc : locations) {
                    Range<Long> locRange = Range.closed(loc.getOffset(), loc.getOffset() + loc.getLength());
                    missingRanges.add(locRange);
                }
                continue;
            }

            log(KeyValue.keyToString(key) + "| " + (i * 100 / index.getLength()) + "%");
        }

    }

    private static byte[] readBlockKey(FSDataInputStream stream, long offset) throws IOException {

        stream.seek(offset);

        byte[] magic = new byte[C.DATABLOCKMAGIC.length];
        stream.readFully(magic);
        if (!Arrays.equals(magic, C.DATABLOCKMAGIC)) {
            log("ERROR: data block does not start with DATABLOCKMAGIC at offset=" + offset);
            exit(2);
        }
        int keyLength = stream.readInt();
        stream.readInt();
        byte[] key = new byte[keyLength];
        stream.readFully(key);

        return key;
    }

}
