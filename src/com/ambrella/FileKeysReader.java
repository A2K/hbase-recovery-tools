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

import static com.ambrella.Log.log;

public class FileKeysReader {

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

        Utils.Index index = Utils.readHFileIndex(conf, file);
        int lastIndex = (int) (index.getLength() - 1);

        long dataEnd = index.getOffset(lastIndex) + index.getSize(lastIndex);

        RangeSet<Long> missingRanges = TreeRangeSet.create();


        outer:
        for(int i = 0; stream.getPos() < dataEnd; i++) {

            BlockLocation[] locations = fs.getFileBlockLocations(fs.getFileStatus(file), stream.getPos(), stream.getPos() + 8);

            for (BlockLocation loc : locations) {
                Range<Long> range = Range.<Long>closed(loc.getOffset(), loc.getOffset() + loc.getLength());
                if (!missingRanges.subRangeSet(range).isEmpty()) {
                    log("skipping input block in non-readable range:", range);
                    continue outer;
                }
            }

            try {

                HFileDataBlock dataBlock = HFileDataBlock.read(stream, (int) index.getSize(i));

                for (HFileDataBlockRecord record: dataBlock.getRecords()) {
                    log("index=" + i + "; key=" + KeyValue.keyToString(record.key));
                }

            } catch(IOException e) {
                for (BlockLocation loc : locations) {
                    Range<Long> range = Range.<Long>closed(loc.getOffset(), loc.getOffset() + loc.getLength());
                    missingRanges.add(range);
                    log("adding missing byte range:", range);
                }
            }
        }

    }

}
