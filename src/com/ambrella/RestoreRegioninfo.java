package com.ambrella;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.MetaScanner;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

public class RestoreRegioninfo {

    public static List<String> readFileList(String fileName) throws IOException {
        FileInputStream ifs;
        try {
            ifs = new FileInputStream(fileName);
        } catch (FileNotFoundException e) {
            System.out.println("Error: " + e.getMessage());
            return null;
        }

        LinkedList<String> files = new LinkedList<String>();
        String line;

        BufferedReader reader = new BufferedReader(new InputStreamReader(ifs));
        while ((line = reader.readLine()) != null) {
            files.add(line);
        }

        reader.close();

        return files;
    }

    public static void main(String[] args) throws IOException {

        if (args.length != 0) {
            log("usage: LIST_FILE_NAME");
            log("LIST_FILE_NAME should be the file with list of missing .regioninfo files to restore");
            System.exit(1);
        }

        String list = args[0];

        if (!new File(list).exists() || !new File(list).isFile()) {
            log("input file " + list + " does not exist or is not a file");
        }

        List<String> files = readFileList(list);

        if (files == null || files.size() == 0) {
            log("no files found to restore in", list);
            System.exit(0);
        }

        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", "mtae1,rdaf3,rdaf2,rdaf1,mtae2");
        conf.addResource(new Path("/etc/hadoop-0.20/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop-0.20/conf/hdfs-site.xml"));
        conf.addResource(new Path("/usr/lib/hbase/conf/hbase-site.xml"));

        FileSystem fs = FileSystem.get(conf);

        List<HRegionInfo> regions = MetaScanner.listAllRegions(conf);

        for (HRegionInfo region : regions) {

            String path = "/hbase/" + new String(region.getTableName()) + "/" + region.getEncodedName() + "/.regioninfo";

            Path filePath = new Path(path);
            if (!fs.exists(filePath)) {
                System.out.println("Found non existing file in metadata: " + path);
            } else {
                if (files.contains(path)) {
                    System.out.println("Fixing .regioninfo: " + path);

                    fs.rename(filePath, filePath.suffix(".old"));

                    FSDataOutputStream stream = fs.create(filePath);
                    region.write(stream);
                    stream.close();

                }
            }

        }

    }

    static void log(Object... objects) {
        for (Object object : objects) {
            System.out.print(object.toString() + " ");
        }
        System.out.println();
    }

}
