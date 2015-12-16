package com.ambrella;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.util.Arrays;

import static com.ambrella.Log.log;

public class Utils {


    final static byte[] DATABLOCKMAGIC = {'D', 'A', 'T', 'A', 'B', 'L', 'K', 42};

    static void exit(int code) {
        System.exit(code);
    }

    public static String formatFileSize(long size) {
        if(size <= 0) return "0";
        final String[] units = new String[] { "B", "kB", "MB", "GB", "TB" };
        int digitGroups = (int) (Math.log10(size)/Math.log10(1024));
        return new DecimalFormat("#,##0.#").format(size/Math.pow(1024, digitGroups)) + " " + units[digitGroups];
    }

    public static class Index {

        private final byte[][] keys;
        private final long[] offsets;
        private final int[] sizes;

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

    public static Index readHFileIndex(Configuration conf, Path filepath) throws IOException, IllegalAccessException, ClassNotFoundException, NoSuchFieldException {
        FileSystem fs = filepath.getFileSystem(conf);
        HFile.Reader reader = new HFile.Reader(fs, filepath, StoreFile.getBlockCache(conf), false);
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


    public static class FixedFileTrailer {
        // Offset to the fileinfo data, a small block of vitals..
        long fileinfoOffset;
        // Offset to the data block index.
        long dataIndexOffset;
        // How many index counts are there (aka: block count)
        int dataIndexCount;
        // Offset to the meta block index.
        long metaIndexOffset;
        // How many meta block index entries (aka: meta block count)
        int metaIndexCount;
        long totalUncompressedBytes;
        int entryCount;
        int compressionCodec;
        int version = 1;

        FixedFileTrailer() {
            super();
        }

        static int trailerSize() {
            // Keep this up to date...
            return
                    ( Bytes.SIZEOF_INT * 5 ) +
                            ( Bytes.SIZEOF_LONG * 4 ) +
                            C.TRAILERBLOCKMAGIC.length;
        }

        void serialize(DataOutputStream outputStream) throws IOException {
            outputStream.write(C.TRAILERBLOCKMAGIC);
            outputStream.writeLong(fileinfoOffset);
            outputStream.writeLong(dataIndexOffset);
            outputStream.writeInt(dataIndexCount);
            outputStream.writeLong(metaIndexOffset);
            outputStream.writeInt(metaIndexCount);
            outputStream.writeLong(totalUncompressedBytes);
            outputStream.writeInt(entryCount);
            outputStream.writeInt(compressionCodec);
            outputStream.writeInt(version);
        }

        void deserialize(DataInputStream inputStream) throws IOException {
            byte [] header = new byte[C.TRAILERBLOCKMAGIC.length];
            inputStream.readFully(header);
            if ( !Arrays.equals(header, C.TRAILERBLOCKMAGIC)) {
                throw new IOException("Trailer 'header' is wrong; does the trailer " +
                        "size match content?");
            }
            fileinfoOffset         = inputStream.readLong();
            dataIndexOffset        = inputStream.readLong();
            dataIndexCount         = inputStream.readInt();

            metaIndexOffset        = inputStream.readLong();
            metaIndexCount         = inputStream.readInt();

            totalUncompressedBytes = inputStream.readLong();
            entryCount             = inputStream.readInt();
            compressionCodec       = inputStream.readInt();
            version                = inputStream.readInt();

            if (version != 1) {
                throw new IOException("Wrong version: " + version);
            }
        }

        @Override
        public String toString() {
            return "fileinfoOffset=" + fileinfoOffset +
                    ", dataIndexOffset=" + dataIndexOffset +
                    ", dataIndexCount=" + dataIndexCount +
                    ", metaIndexOffset=" + metaIndexOffset +
                    ", metaIndexCount=" + metaIndexCount +
                    ", totalBytes=" + totalUncompressedBytes +
                    ", entryCount=" + entryCount +
                    ", version=" + version;
        }
    }


    static class FileInfo extends HbaseMapWritable<byte [], byte []> {
        static final String RESERVED_PREFIX = "hfile.";
        static final byte[] RESERVED_PREFIX_BYTES = Bytes.toBytes(RESERVED_PREFIX);
        static final byte [] LASTKEY = Bytes.toBytes(RESERVED_PREFIX + "LASTKEY");
        static final byte [] AVG_KEY_LEN =
                Bytes.toBytes(RESERVED_PREFIX + "AVG_KEY_LEN");
        static final byte [] AVG_VALUE_LEN =
                Bytes.toBytes(RESERVED_PREFIX + "AVG_VALUE_LEN");
        static final byte [] COMPARATOR =
                Bytes.toBytes(RESERVED_PREFIX + "COMPARATOR");

        /*
         * Constructor.
         */
        FileInfo() {
            super();
        }
    }


}
