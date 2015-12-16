package com.ambrella;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Log {

    public static boolean includeTimestamp = true;

    static void log(Object... objects) {
        if (Log.includeTimestamp) {
            String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").format(new Date());

            System.out.print(time + " ");
        }

        for (Object object : objects) {
            System.out.print(object.toString() + " ");
        }
        System.out.println();
    }

}
