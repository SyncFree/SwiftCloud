package sys.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class FileUtils {

    public static void deleteDir(String dirname) {
        deleteDir(new File(dirname));
    }

    public static void copyDir(String src, String dst) {
        copyDir(new File(src), new File(dst));
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

    public static void copyDir(File src, File dst) {
        if (src.isDirectory()) {
            dst.mkdirs();

            File[] files = src.listFiles();
            if (files != null)
                for (File i : files) {
                    File j = new File(dst.getAbsolutePath() + File.separatorChar + i.getName());
                    if (i.isDirectory())
                        copyDir(i, j);
                    else
                        copyFile(i, j);
                }
        }
    }

    public static void copyFile(File src, File dst) {
        try {
            Files.copy(src.toPath(), dst.toPath(), StandardCopyOption.COPY_ATTRIBUTES,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException x) {
            x.printStackTrace();
        }
    }
}
