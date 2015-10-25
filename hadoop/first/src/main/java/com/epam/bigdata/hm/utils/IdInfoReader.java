package com.epam.bigdata.hm.utils;

import com.epam.bigdata.hm.fs.HDFSManager;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeMap;

public class IdInfoReader {
    private static final String SPLITTER = " ";
    private static final Integer MAX_LINES_COUNT = 1000000;
    private static final String FILE_NAME = "big-";

    private HDFSManager manager;

    public IdInfoReader(HDFSManager manager) {
        this.manager = manager;
    }

    public void readIdInfo(String src, String dest) throws IOException {
        long fileNumber = 0L;
        TreeMap<String, Integer> map = new TreeMap<String, Integer>();

        ArrayList<Path> paths = manager.getFilesPaths(src);

        System.out.println("Started parsing files files from " + src + " into " + dest);
        for (Path path : paths) {
            String sCurrentLine = "";
            BufferedReader reader = null;
            try {
                reader = manager.createBufferedReader(src + path.getName());
                try {
                    while ((sCurrentLine = reader.readLine()) != null) {
                        String ip = sCurrentLine.split("\\t")[2];

                        if (!map.containsKey(ip)) {
                            map.put(ip, 1);
                        } else {
                            Integer count = map.get(ip);
                            map.put(ip, ++count);
                        }

                        if (map.size() >= MAX_LINES_COUNT) {
                            String fileName = dest + FILE_NAME + fileNumber + ".txt";
                            writeIntoFile(fileName, map);
                            map = new TreeMap<String, Integer>();
                            fileNumber++;
                        }
                    }
                } finally {
                    if (reader != null) {
                        reader.close();
                    }
                }
                System.out.println("Parsed = " + path.getName());
            } finally {
                if (reader != null) {
                    reader.close();
                }
            }
        }

        if (map.size() > 0) {
            String fileName = dest + FILE_NAME + fileNumber + ".txt";
            writeIntoFile(fileName, map);
        }
        System.out.println("Finished parsing files files from " + src + " into " + dest);
    }

    private void writeIntoFile(String fileName, TreeMap<String, Integer> map) throws IOException {
        PrintWriter outputStream = null;

        try {
            outputStream = manager.createPrintWriter(fileName);
            Set<String> keys = map.descendingKeySet();
            for (String key : keys) {
                outputStream.println(key + SPLITTER + map.get(key));
            }
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
    }
}