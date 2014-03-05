package sys.utils;

import java.io.File;

public class FileUtils {

    public static void deleteDir(String dirname) {
        deleteDir(new File(dirname));
    }

    public static void deleteDir(File dir) {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null)
                for (File i : files)
                    if (i.isDirectory())
                        deleteDir(i);
                    else
                        i.delete();

            dir.delete();
        }
    }
}
