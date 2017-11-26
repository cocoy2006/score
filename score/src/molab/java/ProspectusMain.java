package molab.java;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import molab.java.util.Util;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class ProspectusMain {
	
	private static final int ATTEMPTS = 5;

	public static void main(String[] args) throws IOException, SQLException {
		if(args.length < 4) {
			throw new IllegalArgumentException("Arguments: <host> <username> <password> <year>");
		}
		if(!Util.isNumberic(args[3]) || Integer.parseInt(args[3]) < 2000) {
			throw new IllegalArgumentException("Argument: <year> not legal.");
		}
		int year = Integer.parseInt(args[3]);
		// 打开mysql连接
		Jdbc jdbc = new Jdbc(args[0], args[1], args[2]);
		// get school code set from database
		Set<Integer> codeSet = new HashSet<Integer>();
		jdbc.getCodes(codeSet);
		// fetch urls
		Map<String, Integer> urls = new LinkedHashMap<String, Integer>();
		String commonUrl = "http://gkcx.eol.cn/schoolhtm/schoolInfo/%d/10071/list_1.htm";
		for(int schCode : codeSet) {
			String url = String.format(commonUrl, schCode);
			int count = 0;
			while(count++ < ATTEMPTS) {
				try {
					Document doc = Jsoup.connect(url).get();
					Elements aList = doc.select(".S_result a");
					if (aList != null && aList.size() > 0) {
						for(Element a : aList) {
							if(a.html().indexOf(String.valueOf(year)) > 0) {
								urls.put("http://gkcx.eol.cn" + a.attr("href"), schCode);
							}
						}
					}
					break;
				} catch (IOException e) {
					try {
						Thread.sleep((long) (Math.random() * 500));
					} catch (InterruptedException e1) {}
				}
			}
		}
		System.out.println("There are " + urls.size() + " urls to fetch.");
		// fetch data
		List<String> sqls = new ArrayList<String>();
		for(String url : urls.keySet()) {
			int count = 0;
			while(count++ < ATTEMPTS) {
				try {
					Document doc = Jsoup.connect(url).get();
					Elements divList = doc.select(".S_result");
					if (divList != null && divList.size() > 0) {
						Element div = divList.get(0);
						sqls.add(generateSql(urls.get(url), year, div.html()));
					}
					break;
				} catch (IOException e) {
					try {
						Thread.sleep((long) (Math.random() * 500));
					} catch (InterruptedException e1) {}
				}
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
	
	private static String generateSql(int schCode, int year, String content) {
		return String.format("INSERT INTO PROSPECTUS(SCH_CODE,YEAR,CONTENT) VALUES(%d,%d,'%s')", schCode, year, content);
	}

}
