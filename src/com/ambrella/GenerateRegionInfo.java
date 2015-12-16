package com.ambrella;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import static com.ambrella.Log.log;
import static com.ambrella.Utils.exit;

// Parses region data files and generates .regioninfo
public class GenerateRegionInfo {

    static final String[] flags = {"-f", "-n", "-r", "-t"};

    private static boolean checkArgs(String[] args) {

        if (args.length < 1 || args.length > flags.length + 1) {
            return false;
        }
        outer:
        for (int i = 1; i < args.length; ++i) {
            String arg = args[i];
            for (String flag : flags) {
                if (arg.equals(flag)) {
                    continue outer;
                }
            }
            return false;
        }

        return true;
    }

    public static void main(String[] args) throws IOException, IllegalAccessException, NoSuchFieldException, ClassNotFoundException {

        if (!checkArgs(args)) {
            Log.includeTimestamp = false;
            log("arguments: REGION [-f] [-n]");
            log("REGION should be an HDFS path of HBase region directory");
            log("Options:");
            log("\t -f \t force .regioninfo overwrite");
            log("\t -n \t do not write any changes");
            log("\t -r \t rename region directory if region name changes");
            log("\t -t \t use current time for region id instead of oldest key timestamp");
            exit(1);
        }

        Path regionDir = new Path(args[0]);

        String[] otherArgs = Arrays.copyOfRange(args, 1, args.length);

        boolean forceOverwrite = false;
        boolean dontWrite = false;
        boolean renameDir = false;
        boolean useCurrentTime = false;
        for (String a : otherArgs) {
            if (a.equals("-f")) {
                log("Got -f argument: will force .regioninfo overwrite");
                forceOverwrite = true;
            } else if (a.equals("-n")) {
                log("Got -n argument: will not actually write any changes");
                dontWrite = true;
            } else if (a.equals("-r")) {
                log("Got -r argument: will rename region directory");
                renameDir = true;
            } else if (a.equals("-t")) {
                log("Got -r argument: will use current time for region timestamp");
                useCurrentTime = true;
            }
        }

        Configuration conf = Config.Hadoop.makeConfig();

        FileSystem fs = regionDir.getFileSystem(conf);

        if (!fs.exists(regionDir) || !fs.getFileStatus(regionDir).isDir()) {
            log("ERROR: region directory does not exist!");
            exit(1);
        }

        Path regionInfoFile = new Path(regionDir, C.REGION_INFO_FILE_NAME);

        if (fs.exists(regionInfoFile)) {
            log(".regioninfo file already exists at path", regionInfoFile);
            if (!forceOverwrite) {
                exit(1);
            } else if (!dontWrite) {
                Path copyPath = new Path("/tmp/.regioninfo-" + regionDir.getName() + ".original");
                log("creating .regioninfo backup at", copyPath);
                FSDataOutputStream copy = fs.create(copyPath);
                int fileSize = (int) fs.getFileStatus(regionInfoFile).getLen();
                byte[] content = new byte[fileSize];
                FSDataInputStream original = fs.open(regionInfoFile);
                original.readFully(content);
                original.close();
                copy.write(content);
                copy.sync();
                copy.close();
            }
        }

        Path attDir = new Path(regionDir, "att");

        if (!fs.exists(attDir) || !fs.getFileStatus(attDir).isDir()) {
            log("ERROR: att directory does not exist in this region");
            exit(1);
        }

        Pair<byte[], byte[]> range = getRegionKeyRange(conf, fs, attDir);
        byte[] minKey = range.getFirst();
        byte[] maxKey = range.getSecond();

        log("region min key:", KeyValue.keyToString(minKey));
        log("region max key:", KeyValue.keyToString(maxKey));

        HTableDescriptor tableDescriptor = new HTableDescriptor(regionDir.getParent().getName());
        tableDescriptor.addFamily(new HColumnDescriptor("att"));

        long regionDate;
        if (useCurrentTime) {
            regionDate = new Date().getTime();
        } else {
            regionDate = getRegionDate(conf, fs, attDir);
        }

        log("region date:", regionDate, "(" + new Date(regionDate).toString() + ")");


        String minKeyFileName = KeyValue.keyToString(minKey).split("/")[0];
        String maxKeyFileName = KeyValue.keyToString(maxKey).split("/")[0];

        HRegionInfo regionInfo = new HRegionInfo(tableDescriptor, Bytes.toBytes(minKeyFileName),
                Bytes.toBytes(maxKeyFileName), false, regionDate);

        log("region name:", regionInfo.getEncodedName());

        if (!dontWrite) {
            FSDataOutputStream out = fs.create(regionInfoFile);
            regionInfo.write(out);
            out.sync();
            out.close();
            log("created .regioninfo:", regionInfoFile);

            if (renameDir) {
                Path newRegionDir = new Path(regionDir.getParent(), regionInfo.getEncodedName());

                if (!newRegionDir.equals(regionDir)) {
                    log("renaming region directory to:", newRegionDir);
                    fs.rename(regionDir, newRegionDir);
                }
            }
        }

        log("done.");

        exit(0);
    }

    private static long getRegionDate(Configuration conf, FileSystem fs, Path attDir) throws IOException, IllegalAccessException, NoSuchFieldException, ClassNotFoundException {

        long earliestTime = new Date().getTime();

        for (FileStatus fileStatus : fs.listStatus(attDir)) {
            Utils.Index index = Utils.readHFileIndex(conf, fileStatus.getPath());
            FSDataInputStream stream = fs.open(fileStatus.getPath());
            HFileDataBlock dataBlock = HFileDataBlock.read(stream, (int) index.getSize(0));
            KeyValue kv = new KeyValue(dataBlock.data);
            long time = kv.getTimestamp();
            if (time < earliestTime) {
                earliestTime = time;
            }
        }

        return earliestTime;
    }

    private static Pair<byte[], byte[]> getRegionKeyRange(Configuration conf, FileSystem fs, Path attDir) throws IOException, IllegalAccessException, ClassNotFoundException, NoSuchFieldException {
        KeyValue.KeyComparator keyComparator = new KeyValue.KeyComparator();

        byte[] minKey = null;
        byte[] maxKey = null;

        for (FileStatus blockFileStatus : fs.listStatus(attDir)) {

            Path blockFilePath = blockFileStatus.getPath();

            log("checking region block file:", blockFilePath);

            Utils.Index index = Utils.readHFileIndex(conf, blockFilePath);

            byte[] blockFirstKey = index.getKey(0);

            byte[] blockLastKey = readTrailerLastKey(fs, blockFileStatus, index);

            log("block first key:", KeyValue.keyToString(blockFirstKey));
            log("block last key:", KeyValue.keyToString(blockLastKey));

            if (minKey == null) {
                minKey = blockFirstKey;
            } else if (keyComparator.compare(minKey, blockFirstKey) > 0) {
                minKey = blockFirstKey;
            }

            if (maxKey == null) {
                maxKey = blockLastKey;
            } else if (keyComparator.compare(maxKey, blockLastKey) < 0) {
                maxKey = blockLastKey;
            }

        }

        return Pair.newPair(minKey, maxKey);
    }

    private static byte[] readTrailerLastKey(FileSystem fs, FileStatus status, Utils.Index index) throws IOException {
        FSDataInputStream stream = fs.open(status.getPath());

        Utils.FixedFileTrailer trailer = new Utils.FixedFileTrailer();
        stream.seek(status.getLen() - Utils.FixedFileTrailer.trailerSize());
        trailer.deserialize(stream);

        stream.seek(trailer.fileinfoOffset);
        Utils.FileInfo fileInfo = new Utils.FileInfo();
        fileInfo.readFields(stream);

        return fileInfo.get(Utils.FileInfo.LASTKEY);
    }

}
