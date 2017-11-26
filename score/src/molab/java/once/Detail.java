package molab.java.once;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import molab.java.Jdbc;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Detail {
	
	private static final int ATTEMPTS = 5;

	public static void main(String[] args) throws IOException, SQLException {
		// open mysql
		Jdbc jdbc = new Jdbc("localhost", "root", "molab,123");
		// get school code set from database
		Set<Integer> codeSet = new HashSet<Integer>();
		jdbc.getCodes(codeSet);
		// fetch data
		List<String> sqls = new ArrayList<String>();
		String commonUrl = "http://gkcx.eol.cn/schoolhtm/schoolInfo/%d/10056/detail.htm";
		for(int schCode : codeSet) {
			String url = String.format(commonUrl, schCode);
			int count = 0;
			while(count++ < ATTEMPTS) {
				try {
					Document doc = Jsoup.connect(url).get();
					Elements divList = doc.select(".S_result");
					if (divList != null && divList.size() > 0) {
						Element div = divList.get(0);
						sqls.add(generateSql(schCode, div.html()));
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
	
	private static String generateSql(int schCode, String detail) {
		return String.format("UPDATE SCHOOL SET DETAIL='%s' WHERE SCH_CODE=%d", detail, schCode);
	}

}
