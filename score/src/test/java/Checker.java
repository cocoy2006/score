package test.java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;

import molab.java.Jdbc;

public class Checker {

	private static Jdbc jdbc;
	private static final String SCH_LOG_FILE = "C:\\Development\\eol\\checker_sch.log";
	private static final String SPE_LOG_FILE = "C:\\Development\\eol\\checker_spe.log";
	
	public static void main(String[] args) throws SQLException {
		jdbc = new Jdbc("localhost", "root", "molab,123");
		// sch_list
		File f = new File("C:\\Development\\eol\\sch_list.log");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(f));
            String temp = "";
            while ((temp = reader.readLine()) != null) {
            	Codes codes = jdbc.getSchoolMap(temp);
            	if(codes != null) {
            		if(!jdbc.getCount("SCH_SCORE_", codes.getProvCode(), codes.getSchCode())) {
    					log(SCH_LOG_FILE, codes.getSchool() + "--" + codes.getSchCode());
    				}
            		if(!jdbc.getCount("SPE_SCORE_", codes.getProvCode(), codes.getSchCode())) {
    					log(SPE_LOG_FILE, codes.getSchool() + "--" + codes.getSchCode());
    				}
        		}
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        
        System.out.println("Done.");
		System.exit(0);
	}
	
	private static void log(String file, String context) {
		FileWriter writer = null;  
        try {     
            // 打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件     
            writer = new FileWriter(file, true);     
            writer.write(context + "\r\n");
        } catch (IOException e) {     
            e.printStackTrace();     
        } finally {     
            try {     
                if(writer != null){  
                    writer.close();     
                }  
            } catch (IOException e) {     
                e.printStackTrace();     
            }     
        }
	}

}
