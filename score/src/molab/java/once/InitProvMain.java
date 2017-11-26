package molab.java.once;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import molab.java.Jdbc;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class InitProvMain {
	
	/**
	 * shanxi 山西
	 * shanxi2 陕西
	 * zhongqing 重庆*/
	public static Map<Integer, String> provinceMap = new HashMap<Integer, String>() {{
		put(10000, "shanghai"); put(10001, "yunnan"); put(10002, "neimenggu");
		put(10003, "beijing"); put(10004, "jilin"); put(10005, "sichuan");
		put(10006, "tianjin"); put(10007, "ningxia"); put(10008, "anhui");
		put(10009, "shandong"); put(10010, "shanxi"); put(10011, "guangdong");
		put(10012, "guangxi"); put(10013, "xinjiang"); put(10014, "jiangsu");
		put(10015, "jiangxi"); put(10016, "hebei"); put(10017, "henan");
		put(10018, "zhejiang"); put(10019, "hainan"); put(10021, "hubei");
		put(10022, "hunan"); put(10023, "gansu"); put(10024, "fujian");
		put(10025, "xizang"); put(10026, "guizhou"); put(10027, "liaoning");
		put(10028, "zhongqing"); put(10029, "shanxi2"); put(10030, "qinghai");
		put(10031, "heilongjiang");
	}};

	public static void main(String[] args) throws IOException, JAXBException, SQLException, DocumentException {
		// open mysql
		Jdbc jdbc = new Jdbc("localhost", "root", "molab,123");
		// 
		List<String> sqls = new ArrayList<String>();
		for(Integer code : provinceMap.keySet()) {
			String prov = provinceMap.get(code);
			String url = "resources/xml/schoolBy_" + prov + ".xml";
			SAXReader reader = new SAXReader();
			Document doc = reader.read(new File(url));
			Element root = doc.getRootElement();
			Iterator schoolIter = root.elementIterator("school");
			while (schoolIter.hasNext()) {
				Element school = (Element) schoolIter.next();
				sqls.add(generateSql(code, Integer.parseInt(school.elementTextTrim("id")), school.elementTextTrim("name")));
			}
		}
		System.out.println("There are " + sqls.size() + " records to insert.");
		// start batch update
		long start = System.currentTimeMillis();
		jdbc.batchUpdate(sqls);
		long end = System.currentTimeMillis() - start;
		System.out.println("Batch update successfully. Time is " + end + " millseconds.");
		jdbc.close();
		
		System.out.println("Done.");
		System.exit(0);
	}
	
	private static String generateSql(int provCode, int schCode, String school) {
		return String.format("INSERT INTO SCHOOL(PROV_CODE,SCH_CODE,SCHOOL) VALUES(%d,%d,'%s')", provCode, schCode, school);
	}
	
}
