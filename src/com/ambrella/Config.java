package com.ambrella;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class Config {

    public static final String TAG = Config.class.getName();

    public static class Hadoop {
        public static final String ZOOKEEPER_QUORUM = "mtae1,rdaf3,rdaf2,rdaf1,mtae2";
        public static final String CORE_CONFIG = "/etc/hadoop/conf/core-site.xml";
        public static final String HDFS_CONFIG = "/etc/hadoop/conf/hdfs-site.xml";
        public static final String HBASE_CONFIG = "/usr/lib/hbase/conf/hbase-site.xml";

        public static Configuration makeConfig() {
            Configuration conf = HBaseConfiguration.create();

            conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
            conf.addResource(new Path(CORE_CONFIG));
            conf.addResource(new Path(HDFS_CONFIG));
            conf.addResource(new Path(HBASE_CONFIG));

            return conf;
        }
    }



}
