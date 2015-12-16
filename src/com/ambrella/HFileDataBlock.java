package com.ambrella;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HFileDataBlock {

    public byte[] data;

    private List<HFileDataBlockRecord> records = null;

    public static HFileDataBlock read(FSDataInputStream stream, int dataBlockSize) throws IOException {
        HFileDataBlock block = new HFileDataBlock();
        byte[] magic = new byte[C.DATABLOCKMAGIC.length];
        stream.readFully(magic);
        if (!Arrays.equals(magic, C.DATABLOCKMAGIC)) {
            throw new IOException("Invalid datablock magic");
        }
        block.data = new byte[dataBlockSize - C.DATABLOCKMAGIC.length];
        stream.readFully(block.data);
        return block;
    }

    public void write(FSDataOutputStream stream) throws IOException {
        stream.write(C.DATABLOCKMAGIC);
        stream.write(data);
    }

    public synchronized List<HFileDataBlockRecord> getRecords() throws IOException {
        if (this.records == null) {
            this.records = HFileDataBlockRecord.splitRecords(this);
        }

        return this.records;
    }

}
