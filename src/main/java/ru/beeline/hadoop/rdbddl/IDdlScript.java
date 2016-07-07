

package ru.beeline.hadoop.rdbddl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Scanner;

public class IDdlScript {
    public IDdlScript() {
    }

    public static void main(String[] args) throws SQLException, InterruptedException {
        new Scanner(System.in);
        int i = 0;
        int cl = 0;
        ArrayList id = new ArrayList();
        ArrayList type = new ArrayList();
        String servername = "";
        String port = "";
        String basename = "";
        String inst = "";
        String tablename = "";
        String otablename = "";
        String partname = "";
        String dbtype = "";
        String nmservername = "--servername";
        String nmport = "--port";
        String nmbasename = "--databasename";
        String nminst = "--instancename";
        String nmtablename = "--tablename";
        String nmotablename = "--outtablename";
        String nmpartname = "--partition";
        String nmdbtype = "--dbtype";
        String nmcolumntype = "--mapclname";

        while(i < args.length) {
            try {
                if(args[i].equals("--help")) {
                    printUsage();
                }

                if(args[i].equals(nmservername)) {
                    ++i;
                    servername = args[i];
                }

                if(args[i].equals(nmport)) {
                    ++i;
                    port = args[i];
                }

                if(args[i].equals(nmbasename)) {
                    ++i;
                    basename = args[i];
                }

                if(args[i].equals(nminst)) {
                    ++i;
                    inst = "\\" + args[i];
                }

                if(args[i].equals(nmtablename)) {
                    ++i;
                    tablename = args[i];
                }

                if(args[i].equals(nmotablename)) {
                    ++i;
                    otablename = args[i];
                }

                if(args[i].equals(nmpartname)) {
                    ++i;
                    partname = args[i];
                }

                if(args[i].equals(nmdbtype)) {
                    ++i;
                    dbtype = args[i];
                }

                if(args[i].equals(nmcolumntype)) {
                    ++i;
                    id.add(args[i]);
                    ++i;
                    type.add(args[i]);
                    ++cl;
                }

                ++i;
            } catch (NumberFormatException var24) {
                System.out.println("ERROR: Integer expected instead of " + args[i]);
                printUsage();
            } catch (ArrayIndexOutOfBoundsException var25) {
                System.out.println("ERROR: Required parameter missing from " + args[i - 1]);
                printUsage();
            }
        }

        if(!partname.equals("")) {
            partname = "PARTITIONED BY (" + partname + " String)";
        }

        if(dbtype.equals("postgres")) {
            CheckPG(inst, servername, port, basename, tablename, otablename, partname, id, type, cl);
        }

        if(dbtype.equals("mysql")) {
            CheckMQL(inst, servername, port, basename, tablename, otablename, partname, id, type, cl);
        }

    }

    public static String CheckPG(String inst, String servername, String port, String basename, String tablename, String otablename, String partname, ArrayList<String> id, ArrayList<String> type, int cl) throws SQLException, InterruptedException {
        boolean j = false;
        int k = 0;
        Connection dbh = DriverManager.getConnection("jdbc:postgresql://" + servername + inst+":" + port + "/" + basename, "postgres", "123123");
        Statement st = dbh.createStatement(1004, 1007);
        ResultSet rs = st.executeQuery("select table_name,column_name, data_type from information_schema.columns where table_schema=\'public\' AND   table_name = \'" + tablename + "\' ;");
        rs.last();
        int i = rs.getRow();
        rs.beforeFirst();
        String res = "CREATE TABLE " + otablename + "(";

        while(rs.next()) {
            ++k;
            int var18 = 0;

            boolean check;
            for(check = false; var18 < type.size(); ++var18) {
                if(rs.getString(2).equals(id.get(var18))) {
                    check = true;
                    break;
                }
            }

            res = res + " " + rs.getString(2) + " ";
            if(check) {
                res = res + (String)type.get(var18) + " ";
            } else if(rs.getString(3).equals("character")) {
                res = res + "STRING ";
            } else if(rs.getString(3).equals("integer")) {
                res = res + "INT ";
            } else {
                res = res + rs.getString(3) + " ";
            }

            if(k < i) {
                res = res + ",";
            }
        }

        res = res + ") " + partname + ";";
        System.out.print(res);
        rs.close();
        st.close();
        return res;
    }

    public static String CheckMQL(String inst, String servername, String port, String basename, String tablename, String otablename, String partname, ArrayList<String> id, ArrayList<String> type, int cl) throws SQLException, InterruptedException {
        boolean j = false;
        int k = 0;
        Connection dbh = DriverManager.getConnection("jdbc:mysql://" + servername + inst +":" + port + "/" + basename, "localhost", "123123");
        Statement st = dbh.createStatement(1004, 1007);
        ResultSet rs = st.executeQuery("SHOW COLUMNS FROM " + tablename + " ;");
        rs.last();
        int i = rs.getRow();
        rs.beforeFirst();
        String res = "CREATE TABLE " + otablename + "(";

        while(rs.next()) {
            ++k;
            int var18 = 0;

            boolean check;
            for(check = false; var18 < type.size(); ++var18) {
                if(rs.getString(1).equals(id.get(var18))) {
                    check = true;
                    break;
                }
            }

            res = res + " " + rs.getString(1) + " ";
            if(check) {
                res = res + (String)type.get(var18) + " ";
            } else if(rs.getString(2).equals("character")) {
                res = res + "STRING ";
            } else if(rs.getString(2).equals("integer")) {
                res = res + "INT ";
            } else {
                res = res + rs.getString(2) + " ";
            }

            if(k < i) {
                res = res + ",";
            }
        }

        res = res + ") " + partname + ";";
        System.out.print(res);
        rs.close();
        st.close();
        return res;
    }

    public static String CheckOQL(String inst, String servername, String port, String basename, String tablename, String otablename, String partname) throws SQLException, InterruptedException {
        int k = 0;
        Connection dbh = DriverManager.getConnection("jdbc:oracle://" + servername + ":" + port + "/" + basename, "postgres", "123123");
        Statement st = dbh.createStatement(1004, 1007);
        ResultSet rs = st.executeQuery("SHOW COLUMNS FROM" + tablename + " ;");
        String s = "CREATE TABLE " + otablename + "(";
        rs.last();
        int i = rs.getRow();
        rs.beforeFirst();

        while(rs.next()) {
            ++k;
            s = s + " " + rs.getString(1) + " ";
            if(rs.getString(2).substring(1, 4).equals("char")) {
                s = s + "STRING ";
            } else if(rs.getString(2).substring(1, 3).equals("var")) {
                s = s + "STRING ";
            } else if(rs.getString(2).substring(1, 4).equals("text")) {
                s = s + "STRING  ";
            } else if(rs.getString(2).substring(1, 3).equals("int")) {
                s = s + "INT ";
            } else if(rs.getString(2).substring(1, 3).equals("int")) {
                s = s + "INT ";
            } else {
                s = s + rs.getString(2) + " ";
            }

            if(k < i) {
                s = s + ",";
            }
        }

        s = s + ");";
        System.out.print(s);
        rs.close();
        st.close();
        return s;
    }

    static void printUsage() {
        System.out.println("MergeForKey [--help]\n  [--servername] - name of server\n  [--basename] - name of database\n  [--port] - database port\n  [--instancename] - instance\n  [--partition] - name of partition column\n  [--tablename] - name of RDB table\n  [--outtablename] - name of HIVE table\n  [--dbtype] - database type (postgres, oracle or mysql)\n  [--mapclname] - type for column with chosen id\n");
        System.exit(1);
    }
}
