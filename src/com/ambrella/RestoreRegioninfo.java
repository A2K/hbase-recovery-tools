package com.ambrella;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.MetaScanner;

import java.io.IOException;
import java.util.List;

import static com.ambrella.Log.log;
import static com.ambrella.Utils.exit;

// Recreates .regioninfo using data from META table
public class RestoreRegionInfo {

    public static void main(String[] args) throws IOException {

        if (args.length != 1) {
            log("args: REGION");
            log("REGION should be HDFS path of HBase region directory");
            exit(1);
        }

        Path regionDir = new Path(args[0]);


        Configuration conf = Config.Hadoop.makeConfig();
        FileSystem fs = FileSystem.get(conf);

        if (!fs.exists(regionDir) || !fs.getFileStatus(regionDir).isDir()) {
            log("invalid region directory");
            exit(1);
        }

        List<HRegionInfo> regions = MetaScanner.listAllRegions(conf);

        for (HRegionInfo region : regions) {

            if (region.getEncodedName().equals(regionDir.getName())) {

                log("found region with matching name in metadata");
                Path regionInfoPath = new Path(regionDir, C.REGION_INFO_FILE_NAME);

                if (fs.exists(regionInfoPath)) {
                    Path backupName = new Path(regionDir, "_old_regioninfo");
                    log("renamed existing .regioninfo to", backupName);
                    fs.rename(regionInfoPath, backupName);
                }

                FSDataOutputStream stream = fs.create(regionInfoPath);
                region.write(stream);

                log("wrote .regioninfo content to", regionInfoPath);
                stream.sync();
                stream.close();
                fs.close();
                exit(0);

            }

        }

    }

}
