package com.epam.bigdata.hm;

import com.epam.bigdata.hm.fs.HDFSManager;
import com.epam.bigdata.hm.utils.ExternalFilesMerge;
import com.epam.bigdata.hm.utils.IdInfoReader;

import java.util.Date;

public class Runner {
    private static final String LOCAL_SORTED_DST = "/user/root/sorted/";

    public static void main(String... args) throws Exception {
        System.out.println("Start date = " + new Date());

        Long startTime = System.currentTimeMillis();
        measure();

        String src = args[0];
        String dest = args[1];

        HDFSManager manager = new HDFSManager();

        IdInfoReader reader = new IdInfoReader(manager);
        reader.readIdInfo(src, LOCAL_SORTED_DST);

        measure();

        ExternalFilesMerge filesMerge = new ExternalFilesMerge(manager);
        filesMerge.externalMerge(LOCAL_SORTED_DST, dest);

        measure();

        Long finishTime = System.currentTimeMillis();
        Long timeDiff = finishTime - startTime;

        measure();

        System.out.println("End date = " + new Date());
        System.out.println("Total time in ms: " + timeDiff);
    }

    private static void measure() {
        double totalMemory = (double) (Runtime.getRuntime().totalMemory());
        double freeMemory = (double) (Runtime.getRuntime().freeMemory());
        double diff = totalMemory - freeMemory;
        System.out.println("Total memory = " + totalMemory + "; free memory = " + freeMemory + "; diff memory = " + diff);
    }
}