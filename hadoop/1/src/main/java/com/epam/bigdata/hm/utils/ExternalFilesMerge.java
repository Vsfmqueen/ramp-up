package com.epam.bigdata.hm.utils;

import com.epam.bigdata.hm.fs.HDFSManager;
import com.epam.bigdata.hm.model.IdInfo;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.util.ArrayList;

public class ExternalFilesMerge {
    private static final String SPLITTER = " ";

    private HDFSManager manager;

    public ExternalFilesMerge(HDFSManager manager) {
        this.manager = manager;
    }

    public void externalMerge(String src, String dest) throws Exception {
        System.out.println("Started merging files from " + src + " into " + dest);

        ArrayList<Path> paths = manager.getFilesPaths(src);

        ArrayList<BufferedReader> readers = new ArrayList();
        PrintWriter writer = null;
        try {

            IdInfo[] lines = new IdInfo[paths.size()];

            for (int i = 0; i < paths.size(); i++) {
                BufferedReader bfr = manager.createBufferedReader(src + paths.get(i).getName());
                String[] nextLine = bfr.readLine().split(SPLITTER);
                IdInfo nextInfo = new IdInfo(nextLine[0], Long.parseLong(nextLine[1]));
                lines[i] = nextInfo;
                readers.add(bfr);
            }

            int length = readers.size();

            writer = manager.createPrintWriter(dest);

            while (length != 0) {
                IdInfo max = null;
                int maxIndex = 0;

                for (int i = 0; i < lines.length; i++) {

                    IdInfo current = lines[i];

                    //case when reader has already read all lines from file
                    if (current == null) {
                        continue;
                    }

                    //the first element from array
                    if (max == null) {
                        max = current;
                        maxIndex = i;
                        continue;

                    } else {

                        int result = max.getIp().compareTo(current.getIp());

                        //the next element is larger than the max one
                        if (result < 0) {
                            max = current;
                            maxIndex = i;
                            //the next element is equal to the max one
                        } else if (result == 0) {
                            Long previousCount = current.getCount();
                            max.setCount(max.getCount() + previousCount);

                            String nextLine = readers.get(i).readLine();
                            if (nextLine == null) {
                                length--;
                                lines[i] = null;
                            } else {
                                lines[i] = nextIpInfo(nextLine);
                            }
                        }
                    }
                }

                //writing out the max element
                String maxLine = readers.get(maxIndex).readLine();
                if (maxLine == null) {
                    length--;
                    lines[maxIndex] = null;
                } else {
                    lines[maxIndex] = nextIpInfo(maxLine);
                }
                writer.println(max.toString());
            }

        } finally {
            if (writer != null) {
                writer.close();
            }
            for (BufferedReader reader : readers) {
                if (reader != null) {
                    reader.close();
                }
            }
        }

        System.out.println("Finished merging files from " + src + " into " + dest);
    }

    private static IdInfo nextIpInfo(String nextLine) {
        String[] nextInfo = nextLine.split(SPLITTER);
        return new IdInfo(nextInfo[0], Long.parseLong(nextInfo[1]));
    }
}
