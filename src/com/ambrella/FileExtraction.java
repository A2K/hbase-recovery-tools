package com.ambrella;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class FileExtraction {

    public static final String TAG = FileExtraction.class.getName();

    private static ClientProtocol namenode = null;

    final static byte[] DATABLOCKMAGIC =
            {'D', 'A', 'T', 'A', 'B', 'L', 'K', 42};

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
            log("Extract all files from HBase file to local directory:");
            log("\tINPUT_FILE -d OUTPUT_DIRECTORY");
            log("Extract a single file from HBase to current directory:");
            log("\tINPUT_FILE -f FILENAME");
            System.exit(1);
        }

        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", "mtae1,rdaf3,rdaf2,rdaf1,mtae2");

        //zkQuorum = ZKConfig.getZKQuorumServersString(conf);
        conf.addResource(new Path("/etc/hadoop-0.20/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop-0.20/conf/hdfs-site.xml"));
        conf.addResource(new Path("/usr/lib/hbase/conf/hbase-site.xml"));
        conf.set("dfs.block.size", "268435456");

        Path file = new Path(args[0]);
        boolean singleFile = args[1].equals("-f");
        String outputFileName = args[2];
        String outputPath = new File(args[2]).getAbsolutePath();

        if (!singleFile) {
            File dir = new File(outputPath);

            if (!dir.exists()) {
                if (!dir.mkdirs()) {

                    log("failed to create output directory:", new Path(outputPath));
                    System.exit(1);
                }
            } else {
                if (!dir.isDirectory()) {
                    log("output directory exists and is a file:", outputPath);
                    System.exit(1);
                }
            }


            System.out.println("Reading: " + file + ", writing to dir: " + outputPath);
        } else {
            System.out.println("Reading: " + file + ", looking for file name: " + outputPath);
        }

        FileSystem fs = file.getFileSystem(conf);

        if (!fs.exists(file)) {
            System.err.println("Input file does not exist in HBase: " + file);
            return;
        }

        HFile.Reader reader = new HFile.Reader(fs, file, null, false);

        Map<byte[], byte[]> fileInfo = reader.loadFileInfo();

        long totalSize = fs.getLength(file);

        List<LocatedBlock> missing = findMissingBlocks(file.toString(), fs.getFileStatus(file).getLen(), conf);
        System.out.println("Found " + missing.size() + " missing blocks in " + file);


        Field f = null;
        try {
            f = reader.getClass().getDeclaredField("blockIndex");
        } catch (NoSuchFieldException e) {
            System.out.println("Error: " + e.getMessage());
            return;
        }

        f.setAccessible(true);
        Object blockIndex = f.get(reader);

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


        for (int i = 0; i < keys.length; ++i) {
            byte[] key = keys[i];

            String keyFileName = KeyValue.keyToString(key).split("/att:data/")[0];

            if (singleFile) {
                if (!keyFileName.matches(outputFileName)) {
                    continue;
                }
            } else {
                log("Found file with matching key: " + outputPath);
            }

            inputStream.seek(offsets[i]);
            inputStream.skipBytes(DATABLOCKMAGIC.length);
            int keyLength = inputStream.readInt();
            int valueLength = inputStream.readInt();
            inputStream.skipBytes(keyLength);
            byte[] value = new byte[valueLength];
            inputStream.readFully(value);

            String outputFilePath = (singleFile ? "./" : (outputPath + "/")) + keyFileName;

            FileOutputStream out = new FileOutputStream(outputFilePath);
            out.write(value);
            out.close();
            log("Wrote file: " + new Path(outputFilePath).getName());

            double progress = ((offsets[i] + sizes[i]) * 10000.0 / totalSize) / 100.0;

            if (singleFile) {
                break;
            }

            log(String.format("progress: %.02f%%", progress));

        }

        inputStream.close();

        log("done.");

        System.exit(0);
    }

    static void log(Object... objects) {
        for (Object object : objects) {
            System.out.print(object.toString() + " ");
        }
        System.out.println();
    }
}
