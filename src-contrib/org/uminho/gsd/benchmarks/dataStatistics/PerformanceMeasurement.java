package org.uminho.gsd.benchmarks.dataStatistics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;


public class PerformanceMeasurement {

    public static int app_pid = 0;

    public static void monitor(double threshold) throws Exception {

        String[] pid_info = ManagementFactory.getRuntimeMXBean().getName().split("@");

        int pid = Integer.parseInt(pid_info[0].trim());

        app_pid = pid;

        try {
            String[] cmd =
                    {
                            "top", "-b", "-H"
                    };

            Runtime rt = Runtime.getRuntime();
            Process proc = rt.exec(cmd);

            TopParser errorGobbler = new
                    TopParser(proc.getErrorStream(), "ERROR (top)", threshold);

            TopParser outputGobbler = new
                    TopParser(proc.getInputStream(), "OUTPUT", threshold);

            errorGobbler.start();
            outputGobbler.start();

        } catch (Throwable t) {
            t.printStackTrace();
        }


    }


    public static void invokeStack(int app_pid,int t_pid) {

        try {
            String[] cmd =
                    {
                            "jstack", app_pid+""
                    };

            Runtime rt = Runtime.getRuntime();
            Process proc = rt.exec(cmd);
            // any error message?
            StackParser errorGobbler = new
                    StackParser(proc.getErrorStream(), "ERROR (jstack)",0);

            // any output?
            StackParser outputGobbler = new
                    StackParser(proc.getInputStream(), "OUTPUT", t_pid);

            // kick them off
            errorGobbler.start();
            outputGobbler.start();

            // any error???
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }


}

class TopParser extends Thread {

    double threshold;
    InputStream is;
    String type;

    TopParser(InputStream is, String type, double threshold) {
        this.is = is;
        this.type = type;
        this.threshold = threshold;
    }

    public void run() {

        if (type.equals("OUTPUT")) {

            int pid_position = 0;
            int cpu_position = 8;
            int cmd_position = 11;

            try {
                InputStreamReader isr = new InputStreamReader(is);
                BufferedReader br = new BufferedReader(isr);
                String line;
                while ((line = br.readLine()) != null) {

                    if (line.contains("PID") && line.contains("USER")) {

                        String[] info = line.split(" ");
                        int i = 0;

                        for (String f : info) {


                            if (f.trim().isEmpty()) {
                                i--;
                            } else {

                                if (f.contains("PID")) {
                                    pid_position = i;
                                }
                                if (f.contains("%CPU")) {
                                    cpu_position = i;
                                }
                                if (f.contains("COMMAND")) {
                                    cmd_position = i;
                                }
                            }
                            i++;
                        }
                    }

                    if (line.contains("java")) {

                        String[] info = line.split(" ");
                        int i = 0;

                        int real_pid_position = 0;
                        int real_cpu_position = 0;
                        int real_cmd_position = 0;
                        int real_i = 0;
                        for (String f : info) {


                            if (f.trim().isEmpty()) {
                                i--;
                            } else {

                                if (i == pid_position) {
                                    real_pid_position = real_i;
                                }
                                if (i == cpu_position) {
                                    real_cpu_position = real_i;
                                }
                                if (i == cmd_position) {
                                    real_cmd_position = real_i;
                                }
                            }
                            real_i++;
                            i++;
                        }

                        double cpu_level = Double.parseDouble(info[real_cpu_position].trim());
                        int pid = Integer.parseInt(info[real_pid_position].trim());
                        if (cpu_level > threshold) {
                            PerformanceMeasurement.invokeStack(PerformanceMeasurement.app_pid,pid);
                        }

                    }

                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        } else if (type.contains("ERROR")) {
            try {
                InputStreamReader isr = new InputStreamReader(is);
                BufferedReader br = new BufferedReader(isr);
                String line = null;
                while ((line = br.readLine()) != null)
                    System.out.println(type + ">" + line);
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }

        }
    }
}

class StackParser extends Thread {

    int t_pid;
    InputStream is;
    String type;

    StackParser(InputStream is, String type, int t_pid) {
        this.is = is;
        this.type = type;
        this.t_pid = t_pid;
    }

    public void run() {


        if (type.equals("OUTPUT")) {


            try {
                InputStreamReader isr = new InputStreamReader(is);
                BufferedReader br = new BufferedReader(isr);
                String line;

                boolean tracking = false;

                StringBuffer stack_trace = new StringBuffer();
                int id = 0;
                String name = "";

                while ((line = br.readLine()) != null) {


                    if (tracking) {
                        stack_trace.append(line + "\n");
                    }


                    if (line.contains("tid=") && line.contains("nid=")) {

                        tracking = true;

                        String[] info = line.split(" ");

                        name = info[0];
                        if (!info[0].endsWith("\"")) {
                            info[0] = "";
                        } else {
                            info[0] = "\"";
                        }

                        for (String f : info) {
                            name += f;
                            if (f.endsWith("\"")) {
                                break;
                            }
                        }

                        for (String f : info) {
                            if (f.contains("nid")) {
                                String[] tid = f.split("0x");
                                id = Integer.parseInt(tid[1], 16);

                            }
                        }


                    }

                    if (line.trim().equals("")) {
                        tracking = false;

                        if(id == t_pid){
                            System.out.println("ID:" + id + " Name: " + name + " \n  ST:" + stack_trace.toString());
                        }

                        stack_trace = new StringBuffer();
                    }

                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }

        } else if (type.contains("ERROR")) {
            try {
                InputStreamReader isr = new InputStreamReader(is);
                BufferedReader br = new BufferedReader(isr);
                String line = null;
                while ((line = br.readLine()) != null)
                    System.out.println(type + ">" + line);
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }

        }
    }
}
