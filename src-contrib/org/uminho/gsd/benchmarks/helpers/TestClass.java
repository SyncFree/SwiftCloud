/*
 * *********************************************************************
 * Copyright (c) 2010 Pedro Gomes and Universidade do Minho.
 * All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************
 */

package org.uminho.gsd.benchmarks.helpers;

//import org.apache.cassandra.thrift.*;
//import org.apache.thrift.TException;
//import org.apache.thrift.transport.TFramedTransport;
//import org.apache.thrift.transport.TSocket;
//import org.apache.thrift.transport.TTransport;
//import org.apache.thrift.transport.TTransportException;
import org.uminho.gsd.benchmarks.probabilityDistributions.PowerLawDistribution;

import java.io.*;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;


/**
 * Created by IntelliJ IDEA.
 * User: pedro
 * Date: Apr 12, 2010
 * Time: 9:58:15 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestClass {
    private static int number_keys = 0;

    public static final String driverName = "com.mysql.jdbc.Driver";
    //public static final String dbName = "jdbc:mysql://192.168.82.20:3306/TPCW";
    public static final String dbName = "jdbc:mysql://localhost:3306/TPCW";

    //  static Connection con;
    static CountDownLatch barrier;
    public static int num_it = 20;
    public static int threads = 20;


    private static Connection getConnection() throws Exception {
        Class.forName(driverName).newInstance();
        Connection con = DriverManager.getConnection(dbName, "root", "naosei");
        con.setAutoCommit(false);
        return con;
    }


//    public static String ObjectToString(Object object) {
//
//        ByteArrayOutputStream bos = new ByteArrayOutputStream();
//        try {
//            ObjectOutputStream oos = null;
//
//            oos = new ObjectOutputStream(bos);
//
//            oos.writeObject(object);
//            String name = new String(bos.toByteArray());
//            return name;
//        } catch (IOException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//        return null;
//    }


    public static void main(String[] args) {

//            String host = "192.168.82.20";
//            int port = 9160;
//
//            TTransport transport = new TFramedTransport(new TSocket(host, port));
//        try {
//            transport.open();
//        } catch (TTransportException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//        TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(transport);
//
//            Cassandra.Client client = new Cassandra.Client(tBinaryProtocol);
//        try {
//            client.set_keyspace("orm");
//            KsDef def = client.describe_keyspace("orm");
//            def.setReplication_factor(3);
//            def.setCf_defs(new ArrayList<CfDef>());
//            client.system_update_keyspace(def);
//        } catch (InvalidRequestException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        } catch (TException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        } catch (NotFoundException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//
//        try {
//            System.out.println(read_from_ColumnFamily(client, "Address", "streetR]zeGmmEP$secj;Riq.Q=street}CgM=GBOVaw-}#EO_yW}RFII|oF.z._oYaOB[MG!;j+sYorg.uminho.gsd.benchmarks.TPCW_CassandraOM.entities.Country@46dab859").toString());
//        } catch (Exception e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }

//        String range = "0,3333";
//        long toExclNo = 0;
//         long fromInclNo  =0 ;
//        if (range != null && !range.equals("")) {
//			// Range is of the format "from, to"
//			String[] fromTo = range.split(",");
//			if (fromTo.length == 2) {
//
//                try {
//					fromInclNo = Long.parseLong(fromTo[0].trim());
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//
//				try {
//                   toExclNo = Long.parseLong(fromTo[1].trim());
//				} catch (Exception e) {
//                    e.printStackTrace();
//
//				}
//
//			} else {
//				fromTo = range.split("to");
//				if (fromTo.length != 2) {
//                    System.out.println("Error");
//				} else {
//					String start_key = fromTo[0].trim();
//					String end_key = fromTo[1].trim();
//				}
//			}
//
//		}

//
//        barrier = new CountDownLatch(threads);
//
//
//
//        try {
//            Connection con = getConnection();
//
////            PreparedStatement statement0 = con.prepareStatement
////            ("INSERT INTO ITEM(i_id,i_stock) VALUES(10,0); ");
////            statement0.execute();
////            con.commit();
//
////                PreparedStatement statement0 = con.prepareStatement
////                  ("SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ ");
////          statement0.execute();
//
//            PreparedStatement statement2 = con.prepareStatement
//                    ("UPDATE ITEM SET i_stock = 0 WHERE i_id = 10");
//            statement2.execute();
//            con.commit();
//
//
//
//        for (int i = 0; i < threads; i++) {
//
//            client client = new client(barrier);
//            Thread t = new Thread(client);
//            t.start();
//        }
//
//        try {
//            barrier.await();
//        } catch (InterruptedException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//
//            PreparedStatement statement = con.prepareStatement
//                        ("SELECT i_stock FROM ITEM WHERE i_id = 10");
//                ResultSet set = statement.executeQuery();
//
//                int stock = -1;
//                while (set.next()) {
//                    stock = set.getInt("i_stock");
//                }
//                if (stock < 0) {
//                    System.out.println("ERRRRROR -FIM");
//                }
//                         con.commit();
//            System.out.println("END STOCK: "+stock);
//
//          } catch (Exception e) {
//            e.printStackTrace();
//        }

//
//        PowerLawDistribution powerLawDistribution = new PowerLawDistribution(10000, 30);
//        //       ZipfDistribution zipfDistribution = new PowerLawDistribution(1000,0.000001);
//
//
//        File file = new File("/Users/pedro/Desktop/Resultados/distribution/dist.csv");
//
//        FileOutputStream out = null;
//        BufferedOutputStream stream = null;
//
//        try {
//            out = new FileOutputStream(file);
//            stream = new BufferedOutputStream(out);
//
//        } catch (Exception e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//        try {
//
//            for (int i = 0; i < 500; i++) {
//                //           System.out.println(zipfDistribution.getNextElement());
//               stream.write((powerLawDistribution.getNextElement() + " \n").getBytes());
//                System.out.println(powerLawDistribution.getNextElement());
//            }
//
////
//            stream.flush();
//            stream.close();
//            out.close();
//
//        } catch (IOException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }

    }


//    public static void main(String[] args) {
//


//        PowerLawDistribution powerLawDistribution = new PowerLawDistribution(1000,0);
//        for(int i =0;i<1000;i++){
//            int next  = powerLawDistribution.getNextElement();
//            System.out.println(next);
//
//        }
//
//    }


//    public static Map<String, Object> read_from_ColumnFamily( Cassandra.Client client, String column_family, Object key) throws Exception {
//
//
//        ColumnParent columnPath = new ColumnParent(column_family);
//
//        ByteBuffer key_bytes = ByteBuffer.wrap(getByteRepresentation(key));
//
//        SlicePredicate slicePredicate = new SlicePredicate();
//        slicePredicate.setSlice_range(new SliceRange(ByteBuffer.wrap(getByteRepresentation("")),ByteBuffer.wrap(getByteRepresentation("")),false,100));
//
//        List<ColumnOrSuperColumn> columnOrSuperColumns;
//
//        columnOrSuperColumns = client.get_slice(key_bytes, columnPath, slicePredicate, ConsistencyLevel.ONE);
//
//        Map<String, Object> results = new TreeMap<String, Object>();
//
//        for (ColumnOrSuperColumn columnOrSuperColumn : columnOrSuperColumns) {
//            results.put(new String(columnOrSuperColumn.getColumn().getName()), columnOrSuperColumn.getColumn().getValue());
//        }
//
//        return results;
//    }
//         private static Object toObject(byte[] bytes) throws IOException, ClassNotFoundException {
//        Object object = null;
//        object = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes)).readObject();
//        return object;
//
//    }

    private static byte[] getByteRepresentation(Object obj) {

        if (obj instanceof Integer) {
            int value = (Integer) obj;
            byte[] bytes = new byte[4];

            bytes[0] = (byte) (value >> 24);
            bytes[1] = (byte) ((value << 8) >> 24);
            bytes[2] = (byte) ((value << 16) >> 24);
            bytes[3] = (byte) ((value << 24) >> 24);

            return bytes;
        }

        if (obj instanceof String) {
            return ((String) obj).getBytes();
        }

        return null;
    }

      public static void getData(String CF, String key){
                    String host = "192.168.82.20";
//            int port = 9160;
//
//            TTransport transport = new TFramedTransport(new TSocket(host, port));
//        try {
//            transport.open();
//        } catch (TTransportException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//        TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(transport);
//
//            Cassandra.Client client = new Cassandra.Client(tBinaryProtocol);
//        try {
//            client.set_keyspace("orm");
////            KsDef def = client.describe_keyspace("orm");
////            def.setReplication_factor(3);
////            def.setCf_defs(new ArrayList<CfDef>());
////            client.system_update_keyspace(def);
//        } catch (InvalidRequestException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        } catch (TException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
////        } catch (NotFoundException e) {
////            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//
//        try {
//            System.out.println(read_from_ColumnFamily(client,CF , key).toString());
//        } catch (Exception e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
    }


    public static void getAddress(String key){
                    String host = "192.168.82.20";
            int port = 9160;
//
//            TTransport transport = new TFramedTransport(new TSocket(host, port));
//        try {
//            transport.open();
//        } catch (TTransportException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//        TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(transport);
//
//            Cassandra.Client client = new Cassandra.Client(tBinaryProtocol);
//        try {
//            client.set_keyspace("orm");
////            KsDef def = client.describe_keyspace("orm");
////            def.setReplication_factor(3);
////            def.setCf_defs(new ArrayList<CfDef>());
////            client.system_update_keyspace(def);
//        } catch (InvalidRequestException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        } catch (TException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
////        } catch (NotFoundException e) {
////            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//
//        try {
//            System.out.println(read_from_ColumnFamily(client,"Address" , key).toString());
//        } catch (Exception e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
    }

}




class client implements Runnable {

    Connection con;
    CountDownLatch barrier;

    public static final String driverName = "com.mysql.jdbc.Driver";
    //public static final String dbName = "jdbc:mysql://192.168.82.20:3306/TPCW";
    public static final String dbName = "jdbc:mysql://localhost:3306/TPCW";


    client(CountDownLatch barrier) {
        this.barrier = barrier;
    }

    private void getConnection() throws Exception {
        Class.forName(driverName).newInstance();
        con = DriverManager.getConnection(dbName, "root", "naosei");
        con.setAutoCommit(false);
        con.setTransactionIsolation(2);

    }


    public void run() {


        try {
            getConnection();

            for (int i = 0; i < TestClass.num_it; i++) {

//                   PreparedStatement statement0 = con.prepareStatement
//                        ("START TRANSACTION");
//                statement0.execute();


                PreparedStatement statement = con.prepareStatement
                        ("UPDATE ITEM SET i_stock = ((SELECT i_stock FROM ITEM WHERE i_id = 10) +1) WHERE i_id = 10");


//                PreparedStatement statement = con.prepareStatement
//                        ("SELECT i_stock FROM ITEM WHERE i_id = 10");
//
//
//                ResultSet set = statement.executeQuery();
//
//                int stock = -1;
//                while (set.next()) {
//                    stock = set.getInt("i_stock");
//                }
//                if (stock < 0) {
//                    System.out.println("ERRRRROR");
//                }
//
//
//                PreparedStatement statement2 = con.prepareStatement
//                        ("UPDATE ITEM SET i_stock = " + (stock+1) + " WHERE i_id = 10");
//                statement2.execute();


//                       PreparedStatement statement3 = con.prepareStatement
//                        ("COMMIT ");
//                statement3.execute();

                //     statement0.close();
                statement.close();
//                statement2.close();
                //     statement3.close();
                con.commit();

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        barrier.countDown();
    }

}

//    public static void main(String[] args) {
//
//        int simulatedDelay = 0;
//
//        Random random = new Random();
//
////        for(int z=0;z<100;z++){
////
////        int num = 10000;
////        int sum = 0;
////
////
////        for (int x = 0; x < num; x++) {
////
////            simulatedDelay = (int) ((-Math.log(random.nextDouble()) * 7) *1000d);
////            if (simulatedDelay > 70000) {
////                simulatedDelay = 70000;
////            }
////            sum+=simulatedDelay;
////       }
////
////        System.out.println("A SD:"+((sum*1d)/(num*1d))/1000d);
////        }
////        TreeMap<Long, String> ll = new TreeMap<Long, String>();
////        ll.put(11l, "1");
////        ll.put(111l, "2");
////        ll.put(2222222l, "6");
////        ll.put(144441l, "5");
////        ll.put(1221l, "3");
////        ll.put(13331l, "4");
////
////        for (Map.Entry<Long, String> entry : ll.entrySet()) {
////
////            System.out.println("I" + entry.getKey());
////
////        }
//
////
//
//
//        //values read form file or hardcoded
//        Map<String, Double> tpcw_mix = new TreeMap<String, Double>();
////        tpcw_mix.put("home", 29.00d);
////        tpcw_mix.put("new_products", 11.00d);
////        tpcw_mix.put("best_sellers", 11.00d);
////        tpcw_mix.put("product_detail", 21.00d);
////        tpcw_mix.put("search", 23.00d);    //Request + confirm
////        tpcw_mix.put("register/login", 0.82d);
////        tpcw_mix.put("order_inquiry", 0.55d);  //Inquiry+ display
////        tpcw_mix.put("admin_change", 0.19d);  //Request + confirm
////        tpcw_mix.put("shoppingCart", 2.00d);
////        tpcw_mix.put("buy_request", 0.75d);
////        tpcw_mix.put("buy_confirm", 0.69d);
//
//                tpcw_mix.put("home", 5.00d);
//        tpcw_mix.put("new_products", 5.00d);
//        tpcw_mix.put("best_sellers", 5.00d);
//        tpcw_mix.put("product_detail", 5.00d);
//        tpcw_mix.put("search", 5.00d);    //Request + confirm
//        tpcw_mix.put("register/login", 5.0d);
//        tpcw_mix.put("order_inquiry", 5.00d);  //Inquiry+ display
//        tpcw_mix.put("admin_change", 5.00d);  //Request + confirm
//        tpcw_mix.put("shoppingCart", 30.00d);
//        tpcw_mix.put("buy_request", 15.75d);
//        tpcw_mix.put("buy_confirm", 15.69d);
//
//        Map<String, Double> workload_values = new TreeMap<String, Double>();
//        double buy_request_prob = 1;
//        double buy_confirm_prob = 1;
//
//        double remaining = 0;
//
//        //Aggregate probabilities for the generation cycle.
//        //As the the values for the buy confirm and buy request are not included, there is a second iteration to add the remaining value to the other options.
//        for (int i = 0; i < 2; i++) {
//            workload_values.clear();
//
//            double aggregated_probability = 0;
//            double prob = tpcw_mix.get("home");
//            workload_values.put("home", prob + (remaining * (prob / 100d)));
//            aggregated_probability = workload_values.get("home");
//
//            prob = tpcw_mix.get("new_products");
//            workload_values.put("new_products", prob + aggregated_probability + (remaining * (prob / 100d)));
//            aggregated_probability += prob + (remaining * (prob / 100d));
//
//            prob = tpcw_mix.get("best_sellers");
//            workload_values.put("best_sellers", prob + aggregated_probability + (remaining * (prob / 100d)));
//            aggregated_probability += prob + (remaining * (prob / 100d));
//
//            prob = tpcw_mix.get("product_detail");
//            workload_values.put("product_detail", prob + aggregated_probability + (remaining * (prob / 100d)));
//            aggregated_probability += prob + (remaining * (prob / 100d));
//
//            prob = tpcw_mix.get("search");
//            workload_values.put("search", prob + aggregated_probability + (remaining * (prob / 100d)));
//            aggregated_probability += prob + (remaining * (prob / 100d));
//
//            prob = tpcw_mix.get("register/login");
//            workload_values.put("register/login", prob + aggregated_probability + (remaining * (prob / 100d)));
//            aggregated_probability += prob + (remaining * (prob / 100d));
//
//            prob = tpcw_mix.get("order_inquiry");
//            workload_values.put("order_inquiry", prob + aggregated_probability + (remaining * (prob / 100d)));
//            aggregated_probability += prob + (remaining * (prob / 100d));
//
//            prob = tpcw_mix.get("admin_change");
//            workload_values.put("admin_change", prob + aggregated_probability + (remaining * (prob / 100d)));
//            aggregated_probability += prob + (remaining * (prob / 100d));
//
//            prob = tpcw_mix.get("shoppingCart");
//            workload_values.put("shoppingCart", prob + aggregated_probability + (remaining * (prob / 100d)));
//            aggregated_probability += prob + (remaining * (prob / 100d));
//
//            remaining = 100 - aggregated_probability;
//            System.out.println("A" + i + "" + (100 - aggregated_probability));
//        }
//
//        //set buy options probabilities. If shopping cart is 2% and the buy request is 0,75% then of each cart there is a 0,41% probability.
//        double sc_prob = tpcw_mix.get("shoppingCart");
//        double br_prob = tpcw_mix.get("buy_request");
//        buy_request_prob = br_prob / sc_prob;
//        double bc_prob = tpcw_mix.get("buy_confirm");
//        buy_confirm_prob = bc_prob / br_prob;
//
//        System.out.println("BR:" + buy_request_prob + "  BC:" + buy_confirm_prob);
//
//        Random rand = new Random();
//
//        int i1 = 0;
//        int i2 = 0;
//        int i3 = 0;
//        int i4 = 0;
//        int i5 = 0;
//        int i6 = 0;
//        int i7 = 0;
//        int i8 = 0;
//        int i9 = 0;
//        int i10 = 0;
//        int i11 = 0;
//
//        int num_operations = 1000000;
//        boolean shopping = false;
//        boolean confirm = false;
//        for (int i = 0; i < num_operations; i++) {
//
//
//            if (shopping) {
//
//                //if confirm
//                if (confirm) {
//                    i11++;
//                    //execute buy confirm operation
//                    shopping = false;
//                    confirm = false;
//                } else {
//                    i10++;
//                    double d = rand.nextDouble();
//                    if (d < buy_confirm_prob) {
//                        //on next iteration do a buy confirm
//                        confirm = true;
//                    } else {
//                        shopping = false;
//                        confirm = false;
//                    }
//                    //execute buy request operation
//                }
//            } else {
//                double d = rand.nextDouble() * 100;
//                if (d < workload_values.get("home")) {
//                    i1++;
//                    //execute home operation
//
//                } else if (d < workload_values.get("new_products")) {
//                    i2++;
//                    //execute new products operation
//
//                } else if (d < workload_values.get("best_sellers")) {
//                    i3++;
//                    //execute best sellers operation
//
//                } else if (d < workload_values.get("product_detail")) {
//                    i4++;
//                    //execute product detail operation
//
//
//                } else if (d < workload_values.get("search")) {
//                    i5++;
//                    //execute product search operation
//
//
//                } else if (d < workload_values.get("register/login")) {
//                    i6++;
//                    double decision_factor = rand.nextDouble();
//
//                    if (decision_factor < 0.2) {
//                       //register customer
//                    }
//                    else {
//                       //do login
//                    }
//
//                } else if (d < workload_values.get("order_inquiry")) {
//                    i7++;
//                    //execute order operation
//
//
//                } else if (d < workload_values.get("admin_change")) {
//                    i8++;
//                    //execute admin operation
//
//                } else if (d < workload_values.get("shoppingCart")) {
//                    i9++;
//                    double d2 = rand.nextDouble();
//                    if (d2 < buy_request_prob) {
//                        //on next operation do a buy request id the probability checks
//                        shopping = true;
//                        confirm = false;
//                    } else {
//                        shopping = false;
//                        confirm = false;
//                    }
//
//                }
//
//            }
//
//        }
//
//        int operation_num = num_operations;
//        System.out.println("-------");
//        System.out.println("OP1 HOME:" + ((i1 / (operation_num * 1d)) * 100));
//        System.out.println("OP2 NewProd:" + ((i2 / (operation_num * 1d)) * 100));
//        System.out.println("OP3 BestSellers:" + ((i3 / (operation_num * 1d)) * 100));
//        System.out.println("OP4 PDetail:" + ((i4 / (operation_num * 1d)) * 100));
//        System.out.println("OP5 Search:" + ((i5 / (operation_num * 1d)) * 100));
//        System.out.println("OP9 ShoppingCart:" + ((i9 / (operation_num * 1d)) * 100));
//        System.out.println("OP6 CostumerRegis:" + ((i6 / (operation_num * 1d)) * 100));
//        System.out.println("OP10 BRequest:" + ((i10 / (operation_num * 1d)) * 100));
//        System.out.println("OP11 BConfirm:" + ((i11 / (operation_num * 1d)) * 100));
//        System.out.println("OP7 OrderInqui:" + ((i7 / (operation_num * 1d)) * 100));
//        System.out.println("OP8 AdminCofirm:" + ((i8 / (operation_num * 1d)) * 100));
//
//        System.out.println("-------");
//
//
//        //      PowerLawDistribution dis = new PowerLawDistribution();
////        TreeMap<String,String> xpto =  new TreeMap<String, String>();
////        xpto.put("alpha",0.0d+"");
////        dis.setInfo(xpto);
////        dis.init(1000,null);
////        for(int i =0;i<1000;i++){
////            System.out.println("DIST:"+dis.getNextElement());
////
////
////        }
//
////
////        TSocket socket = new TSocket("192.168.82.21", 9160);
////        TProtocol prot = new TBinaryProtocol(socket);
////        Cassandra.Client c = new Cassandra.Client(prot);
////        try {
////            socket.open();
////
////            ColumnParent parent = new ColumnParent();
////            parent.setColumn_family("Customer");
////
////            SlicePredicate slice_predicate = new SlicePredicate();
////            List<byte[]> column_names = new ArrayList<byte[]>();
////
////            column_names.add("C_EMAIL".getBytes());
////
////            slice_predicate.setColumn_names(column_names);
////
////
////            KeyRange range = new KeyRange();
////            range.setCount(5000);
////
////            if (true) {
////                range.setStart_key("1.0");
////                range.setEnd_key("");
////            } else {
////                range.setStart_key("");
////                range.setEnd_key("1.0");
////            }
////
////
////            String last_key = "";
////            long limit = Long.MAX_VALUE;
////
////            boolean terminated = false;
////
////            while (!terminated) {
////
////                List<KeySlice> keys = c.get_range_slices("ORM_Tpcw", parent,
////                        slice_predicate, range,
////                        ConsistencyLevel.QUORUM);
////
////                if (!keys.isEmpty()) {
////                    last_key = keys.get(keys.size() - 1).key;
////                    range.setStart_key(last_key);
////                    System.out.println("RK: "+range.start_key +" -- "+range.end_key);
////                }
////
////                for (KeySlice key : keys) {
////                    if (!key.getColumns().isEmpty()) {
////                        number_keys++;
////                        System.out.println("K:" + key.getKey());
////                    }
////                    if (number_keys >= limit) {
////                        terminated = true;
////                        break;
////                    }
////
////                }
////                if (keys.size() < 5000) {
////                    terminated = true;
////                }
////
////
////            }
////
////            System.out.println("N K: " + number_keys);
//
////        java.util.Map<java.lang.String, java.util.Map<java.lang.String, java.lang.String>> map =     c.describe_keyspace("Keyspace1");
////
////            for(String s : map.keySet()){
////                System.out.println(s);
////
////            }
////
////
////
////
////        } catch (TTransportException e) {
////            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
////        } catch (TException e) {
////            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
////        } catch (NotFoundException e) {
////            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
////        }
//
//
////        //TestClass tc = new TestClass();
////      //  tc.init();
////
////
////
////        try {
////
////                   TSocket socket = new TSocket("localhost", 9160);
////		TProtocol protocol = new TBinaryProtocol(socket);
////		Cassandra.Client cassandraClient = new Cassandra.Client(protocol);
////            socket.open();
////
////            List<TokenRange> ring =  cassandraClient.describe_ring("Tpcw");
////            for(TokenRange tr : ring){
////                System.out.println("TR:"+tr.toString());
////                for(String end : tr.endpoints){
////                    System.out.println("EP:"+end);
////
////                }
////
////            }
////
////        } catch (TException e) {
////            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
////        }
//
//
////   byte[]utf8Bytes = new byte[]{-84, -19, 0, 5, 116, 0, 5, 48, 46, 48, 46, 48};
////
////
////        try {
////            String roundTrip = new String(utf8Bytes, "UTF8");
////              System.out.println("roundTrip = " + roundTrip);
////
////        } catch (UnsupportedEncodingException e) {
////            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
////        }
////
////            			ColumnParent parent = new ColumnParent();
////			parent.setColumn_family("orders");
////
////
////         	SlicePredicate slice_predicate = new SlicePredicate();
////			SliceRange range = new SliceRange("".getBytes(),"".getBytes(),false,((int)(100)));
////			slice_predicate.setSlice_range(range);
////
////            KeyRange krange = new KeyRange(100);
////            krange.setStart_key("");
////            krange.setEnd_key("");
////
////            java.util.List<org.apache.cassandra.thrift.KeySlice> key_columns = c.get_range_slices("Tpcw", parent ,  slice_predicate,krange, ConsistencyLevel.QUORUM);
//
//
//        //	List<ColumnOrSuperColumn> key_columns = c.get_slice("Tpcw", "434324r" , parent, slice_predicate, ConsistencyLevel.QUORUM);
//        List<String> keys = new ArrayList<String>();
//
//
////        int i = 1;
////        int y = 2;
////
////
////        String k = ObjectToString(i);
////        System.out.println("K:" + k);
////        String x = k;
////        k = ObjectToString(y);
////        System.out.println("Y:" + k);
////        System.out.println("E:" + (k.equals(x)));
////        } catch (TTransportException e) {
////            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
////        } catch (UnavailableException e) {
////            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
////        } catch (TException e) {
////            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
////        } catch (InvalidRequestException e) {
////            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
////        } catch (TimedOutException e) {
////            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
////        }
//
//
////   CopyOnWriteArrayList<String> items;
////  // CassandraInterface61 DBinterface;{
////        try {
////            DBinterface = new CassandraInterface61();
////        } catch (IOException e) {
////            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
////        }
////    }
////
////    public void init(){
////        getItemsAndStock();
////        StockHandler sh =  new StockHandler(DBinterface,10,items,0,10);
////        sh.init();
////        RClient cl =  new RClient();
////        Thread t = new Thread(cl);
////        t.start();
////        try {
////            t.join();
////        } catch (InterruptedException e) {
////            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
////        }
////        sh.finishHandler();
////        System.out.println("COUNT: "+cl.count);
////
////    }
////
////
////    public void getItemsAndStock() {
////
////           ArrayList<String> fields = new ArrayList<String>();
////           fields.add("I_TITLE");
////           fields.add("I_STOCK");
////           Map<String, Map<String, Object>> items_info = DBinterface.getClient().rangeQuery("Items", fields, -1);
////
////
////           System.out.println("[INFO:] ITEMS COLLECTED FROM THE DATABASE: SIZE = " + items_info.size());
////           items = new CopyOnWriteArrayList<String>();
////           for (String ik : items_info.keySet()) {
////               ArrayList<Object> data = new ArrayList<Object>();
////               items.add(ik);
////           }
////            for(String item: items){
////                DBinterface.getClient().update(item, "Items", "I_STOCK", (long) 10);
////            }
////       }
////
////
////    class RClient implements Runnable{
////
////        int count =0;
////
////        public void run() {
////            DataBaseCRUDInterface.CRUDclient ccl=  DBinterface.getClient();
////
////            for(int i =0;i<20;i++){
////                for(String item:items){
////                     ccl.update(item,"Items","I_STOCK",0l);
////                    count++;
////                    try {
////                        Thread.sleep(10);
////                    } catch (InterruptedException e) {
////                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
////                    }
////                }
////            }
////        }
////    }
////
////  }
//
//
//    }
//}


//        if (option.equals("home")) {
//
//            Map<String, Object> parametros = new TreeMap<String, Object>();
//            parametros.put("ITEM", 400);
//            parametros.put("COSTUMER", 20);
//
//            op = new Operation("OP_HOME", parametros);
//
//        }
//        if (option.equals("shoppingCart")) {
//
//            Map<String, Object> parametros = new TreeMap<String, Object>();
//            parametros.put("CART", nodeID.getId() + "." + private_id + "." + shop_id);
//            parametros.put("ITEM", rand.nextInt(400));
//            parametros.put("CREATE", create);
//
//            op = new Operation("OP_SHOPPING_CART", parametros);
//
//            num_items++;
//            if (num_items > 2) {
//                option = "buy_request";
//            }
//
//            create = false;
//        }


//        if (option.equals("register/login")) {
//
//            Map<String, Object> parametros = new TreeMap<String, Object>();
//
//            float decision_factor = rand.nextFloat();
//            if (decision_factor < 0.2) {
//                parametros.put("CUSTOMER", nodeID.getId() + "." + private_id + "." + customer_id);
//                op = new Operation("OP_REGISTER", parametros);
//                customer_id++;
//            } else {
//                parametros.put("CUSTOMER", rand.nextInt(2500) + "");
//                op = new Operation("OP_LOGIN", parametros);
//            }
//        }

//        if (option.equals("buy_request")) {
//            Map<String, Object> parametros = new TreeMap<String, Object>();
//
//
//            parametros.put("CART", nodeID.getId() + "." + private_id + "." + shop_id);
//            op = new Operation("OP_BUY_REQUEST", parametros);
//
//            option = "buy_confirm";
//            return op;
//
//        }
//        if (option.equals("buy_confirm")) {
//            Map<String, Object> parametros = new TreeMap<String, Object>();
//
//            parametros.put("CART", nodeID.getId() + "." + private_id + "." + shop_id);
//            parametros.put("CUSTOMER", /*rand.nextInt(2400)*/440 + "");
//
//            op = new Operation("OP_BUY_CONFIRM", parametros);
//
//            if (iteration > 4) {
//                option = "admin_change";
//                iteration = 0;
//            } else {
//                option = "shoppingCart";
//                iteration++;
//            }
//
//            shop_id++;
//            num_items = 0;
//            create = true;
//            return op;
//        }
//        if (option.equals("order_inquiry")) {
//            Map<String, Object> parametros = new TreeMap<String, Object>();
//
//            parametros.put("CUSTOMER", /*rand.nextInt(2400)*/440 + "");
//
//            op = new Operation("OP_ORDER_INQUIRY", parametros);
//
//            option = "shoppingCart";
//        }
//        if (option.equals("search")) {
//            if (subject.equals("AUTHOR")) {
//                String subject_field = BenchmarkUtil.getRandomAString(2, 20);
//
//                Map<String, Object> parametros = new TreeMap<String, Object>();
//
//                parametros.put("TERM", subject);
//                parametros.put("FIELD", subject_field);
//
//                op = new Operation("OP_SEARCH", parametros);
//                subject = "TITLE";
//                return op;
//
//
//            } else if (subject.equals("TITLE")) {
//                String subject_field = BenchmarkUtil.getRandomAString(14, 60);
//
//                Map<String, Object> parametros = new TreeMap<String, Object>();
//
//                parametros.put("TERM", subject);
//                parametros.put("FIELD", subject_field);
//
//                op = new Operation("OP_SEARCH", parametros);
//                subject = "SUBJECT";
//                return op;
//
//            } else if (subject.equals("SUBJECT")) {
//                String subject_field = subjects[rand.nextInt(subjects.length)];
//
//                Map<String, Object> parametros = new TreeMap<String, Object>();
//
//                parametros.put("TERM", subject);
//                parametros.put("FIELD", subject_field);
//
//                op = new Operation("OP_SEARCH", parametros);
//                subject = "AUTHOR";
//                return op;
//
//
//            } else {
//
//                System.out.println("OPTION NOT RECOGNIZED");
//            }
//
//
//        }
//
//        if (option.equals("new_products")) {
//            String subject_field = subjects[rand.nextInt(subjects.length)];
//
//            Map<String, Object> parametros = new TreeMap<String, Object>();
//            parametros.put("FIELD", subject_field);
//
//            op = new Operation("OP_NEW_PRODUCTS", parametros);
//            return op;
//        }
//
//        if (option.equals("best_sellers")) {
//            String subject_field = subjects[rand.nextInt(subjects.length)];
//
//            Map<String, Object> parametros = new TreeMap<String, Object>();
//            parametros.put("FIELD", subject_field);
//
//            op = new Operation("OP_BEST_SELLERS", parametros);
//            option = "shoppingCart";
//
//            return op;
//        }
//        if (option.equals("product_detail")) {
//            int item = rand.nextInt(1000);
//
//            Map<String, Object> parametros = new TreeMap<String, Object>();
//            parametros.put("ITEM", item);
//
//            op = new Operation("OP_ITEM_INFO", parametros);
//            return op;
//        }
//        if (option.equals("admin_change")) {
//            int item = rand.nextInt(1000);
//
//            Map<String, Object> parametros = new TreeMap<String, Object>();
//            parametros.put("ITEM", item);
//
//            op = new Operation("OP_ADMIN_CHANGE", parametros);
//            option = "shoppingCart";
//
//            return op;
//        }
//
//./libs/antlr-2.7.6.jar
//./libs/apache-cassandra-0.6.1.jar
//./libs/asm-2.2.1.jar
//./libs/asm-tree-2.2.1.jar
//./libs/bndtask-0.2.0.jar
//./libs/cglib-nodep-2.2.jar
//./libs/cobertura-1.9.jar
//./libs/commons-lang-2.3.jar
//./libs/commons-logging-api-5.5.23.jar
//./libs/asm-3.1.jar
//./libs/datanucleus-cassandra-0.1.jar
//./libs/datanucleus-core-2.1.0-m2.jar
//./libs/datanucleus-enhancer-2.0.2.jar
//./libs/jdo2-api.jar
//./libs/jackson-core-asl-1.0.1.jar
//./libs/jackson-core-lgpl-1.0.1.jar
//./libs/jackson-jaxrs-1.0.1.jar
//./libs/jackson-mapper-asl-1.0.1.jar
//./libs/jackson-mapper-lgpl-1.0.1.jar
//./libs/jakarta-oro-2.0.8.jar
//./libs/jartmp6866002639310898349.tmp
//./libs/json_simple-1.0.2.jar
//./libs/json-org.jar
//./libs/json-rpc-1.0.jar
//./libs/jsontools-core-1.5.jar
//./libs/jsr311-api-1.0.jar
//./libs/junit-4.5.jar
//./libs/libthrift-r917130.jar
//./libs/log4j-1.2.13.jar
//./libs/maven-ant-tasks-2.0.9.jar
//./libs/mysql-connector-java-5.1.12-bin.jar
//./libs/noggit-1.0-SNAPSHOT.jar
//./libs/slf4j-api-1.5.8.jar
//./libs/slf4j-log4j12-1.5.8.jar
//./libs/stringtree-json-2.0.5.jar