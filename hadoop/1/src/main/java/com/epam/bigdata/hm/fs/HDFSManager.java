package com.epam.bigdata.hm.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;

public class HDFSManager {
    FileSystem fs;

    public HDFSManager() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.0.2.15:8020");
        System.setProperty("HADOOP_USER_NAME", "root");

        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );

        fs = FileSystem.get(conf);
    }

    public BufferedReader createBufferedReader(String src) throws IOException {
        Path path = new Path(src);
        return new BufferedReader(new InputStreamReader(fs.open(path)));
    }

    public PrintWriter createPrintWriter(String src) throws IOException {
        FSDataOutputStream stream = fs.create(new Path(src));
        return new PrintWriter(new BufferedWriter(new OutputStreamWriter(stream)));
    }

    public ArrayList<Path> getFilesPaths(String src) throws IOException {
        ArrayList<Path> paths = new ArrayList<Path>();
        FileStatus[] fileStatuses = fs.listStatus(new Path(src));
        for (FileStatus status : fileStatuses) {
            paths.add(status.getPath());
        }
        return paths;
    }
}

