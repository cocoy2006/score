package molab.java.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class Util {
	
	public static final String SCH_LOG = System.getProperty("user.home") + "/sch.log";
	public static final String SCH_ERR_LOG = System.getProperty("user.home") + "/sch_err.log";
	public static final String SCH_AG_LOG = System.getProperty("user.home") + "/sch_ag.log";
	public static final String SCH_AG_ERR_LOG = System.getProperty("user.home") + "/sch_ag_err.log";
	public static final String SPE_LOG = System.getProperty("user.home") + "/spe.log";
	public static final String SPE_ERR_LOG = System.getProperty("user.home") + "/spe_err.log";
	public static final String SPE_AG_LOG = System.getProperty("user.home") + "/spe_ag.log";
	public static final String SPE_AG_ERR_LOG = System.getProperty("user.home") + "/spe_ag_err.log";
	
	public static Set<Integer> build(String file) throws IOException {
		Set<Integer> set = new HashSet<Integer>();
		String codeStr = Util.read(file).trim();
		if(codeStr != "" && codeStr.indexOf(",") > 0) {
			for(String str : codeStr.split(",")) {
				set.add(Integer.parseInt(str));
			}
		}
		return set;
	}
	
	/**
	 * 读取日志文件 
	 * @throws IOException */
	public static String read(String file) throws IOException {
		File f = new File(file);
		if(!f.exists()) {
			f.createNewFile();
			return "";
		}
        BufferedReader reader = null;
        StringBuffer sb = new StringBuffer();
        try {
            reader = new BufferedReader(new FileReader(new File(file)));
            String temp = "";
            while ((temp = reader.readLine()) != null) {
            	sb.append(temp);
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
        return sb.toString();
	}
	
	/**
	 * 写入日志文件，记录已处理或异常的SCH_CODE */
	public static void log(String file, int schCode) {
		FileWriter writer = null;  
        try {     
            // 打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件     
            writer = new FileWriter(file, true);     
            writer.write(String.valueOf(schCode) + ",");       
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

	/**
	 * 字符串是否为有效数字 */
	public static boolean isNumberic(String str) {
		Pattern pattern = Pattern.compile("[0-9]{1,4}");
	    return str != "" && pattern.matcher(str).matches();
	}
}
