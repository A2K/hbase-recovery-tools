package com.ambrella;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class HFileDataBlockRecord {

    public byte[] key;
    public byte[] data;

    public static List<HFileDataBlockRecord> splitRecords(HFileDataBlock dataBlock) throws IOException {

        List<HFileDataBlockRecord> dataBlockRecordList = new LinkedList<HFileDataBlockRecord>();

        DataInputStream stream = new DataInputStream(new ByteArrayInputStream(dataBlock.data));

        long dataBlockPosition = 0;

        while (dataBlockPosition < dataBlock.data.length) {

            int keySize = stream.readInt();
            int dataSize = stream.readInt();

            HFileDataBlockRecord record = new HFileDataBlockRecord();

            record.key = new byte[keySize];
            record.data = new byte[dataSize];

            stream.readFully(record.key);
            stream.readFully(record.data);

            dataBlockRecordList.add(record);

            dataBlockPosition += 4 + 4 + keySize + dataSize;
        }

        return dataBlockRecordList;

    }

}
