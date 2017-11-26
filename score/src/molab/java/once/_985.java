package molab.java.once;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import molab.java.Jdbc;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class _985 {

	public static void main(String[] args) throws IOException, SQLException {
		// fetch 985 data
		List<String> sqls = new ArrayList<String>();
		String url = "http://www.eol.cn/html/g/gxmd/985.shtml";
		Document doc = Jsoup.connect(url).get();
		Elements aList = doc.select(".zk_ad_475 table tr:gt(0) td:eq(0) a");
		if (aList != null && aList.size() > 0) {
			for(Element a : aList) {
				String href = a.attr("href").trim();
				String schCodeStr = href.substring(href.lastIndexOf("/") + 7, href.lastIndexOf("."));
				sqls.add(generateSql(Integer.parseInt(schCodeStr)));
			}
		}
		System.out.println("There are " + sqls.size() + " records to insert.");
		// open mysql
		Jdbc jdbc = new Jdbc("localhost", "root", "molab,123");
		// start batch update
		long start = System.currentTimeMillis();
		jdbc.batchUpdate(sqls);
		long end = System.currentTimeMillis() - start;
		System.out.println("Batch update successfully. Time is " + end + " millseconds.");
		jdbc.close();
		
		System.out.println("Done.");
		System.exit(0);
	}
	
	private static String generateSql(int schCode) {
		return String.format("UPDATE SCHOOL SET NEF=1 WHERE SCH_CODE=%d", schCode);
	}

}
