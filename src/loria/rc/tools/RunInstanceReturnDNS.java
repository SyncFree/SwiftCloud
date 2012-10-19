/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package loria.rc.tools;

import java.io.File;
import java.io.PrintStream;
import loria.rc.AmazonMachine;

/**
 *
 * @author Stephane Martin <stephane@stephanemartin.fr>
 */
public class RunInstanceReturnDNS {
    public static void main(String ...arg) throws Exception {
        File f= new File(arg[0]);
        PrintStream out=new PrintStream(f);
        AmazonMachine ma=new AmazonMachine();
        ma.checkAndCreateSecurityGroup();
        ma.startInstanceRequest(Integer.parseInt(arg[1]));
        ma.waitAllLauched();
        for (String addr:ma.getAdresseDnsMachine()){
            out.println(addr);
        }
        out.close();
    }
}
