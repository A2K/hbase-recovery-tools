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
import org.apache.hadoop.hbase.regionserver.StoreFile;

import java.io.IOException;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class FileKeysReader {

    public static final String TAG = FileKeysReader.class.getName();


    static void log(Object... objects) {
        String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").format(new Date());

        System.out.print(time + " ");

        for (Object object : objects) {
            System.out.print(object.toString() + " ");
        }
        System.out.println();
    }


    final static byte[] DATABLOCKMAGIC = {'D', 'A', 'T', 'A', 'B', 'L', 'K', 42};


    public static void main(String[] args) {

        if (args.length != 1) {
            log("args: FILENAME");
            System.exit(1);
        }

        Path file = new Path(args[0]);

        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", "mtae1,rdaf3,rdaf2,rdaf1,mtae2");

        conf.addResource(new Path("/etc/hadoop-0.20/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop-0.20/conf/hdfs-site.xml"));
        conf.addResource(new Path("/usr/lib/hbase/conf/hbase-site.xml"));
        conf.set("dfs.block.size", "268435456");

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

        Index index = readIndex(conf, file);
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
                byte[] key = readBlockKey(stream);
                log("index=" + i + "; key=" + KeyValue.keyToString(key));
            } catch(IOException e) {
                for (BlockLocation loc : locations) {
                    Range<Long> range = Range.<Long>closed(loc.getOffset(), loc.getOffset() + loc.getLength());
                    missingRanges.add(range);
                    log("adding missing byte range:", range);
                }
            }
        }

    }

    private static byte[] readBlockKey(FSDataInputStream stream) throws IOException {

        long pos = stream.getPos();

        byte[] magic = new byte[DATABLOCKMAGIC.length];
        stream.readFully(magic);
        if (!Arrays.equals(magic, DATABLOCKMAGIC)) {
            stream.seek(pos);
        }

        int keyLength = stream.readInt();
        int dataLength = stream.readInt();
        byte[] key = new byte[keyLength];
        stream.readFully(key);
        byte[] data = new byte[dataLength];
        stream.readFully(data);

        return key;

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
}
