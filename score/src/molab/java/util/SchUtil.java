package molab.java.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class SchUtil {
	
	private static final Integer[] stuProvCodes = {
		10000, 10001, 10002, 10003, 10004, 10005, 10006, 10007, 10008, 10009,
		10010, 10011, 10012, 10013, 10014, 10015, 10016, 10017, 10018, 10019,
		10021, 10022, 10023, 10024, 10025, 10026, 10027, 10028, 10029, 10030, 10031}; // 生源省编号
	private static final Integer[] categorys = {10035, 10034, 10090, 10091, 10093}; // 考生类别编号
	private static final Integer[] batchs = {10036, 10037, 10038, 10148, 10049}; // 录取批次编号
	private static final String SCH_SCORE_URL = "http://gkcx.eol.cn/schoolhtm/schoolAreaPoint/%d/%d/%d/%d.htm#";
	private static final String SCH_SCORE_URL_SUMMARY = "http://gkcx.eol.cn/schoolhtm/schoolAreaPoint/%d/schoolAreaPoint.htm#";
	private static final String SCH_SCORE_URL_LITE = "http://gkcx.eol.cn%s%d.htm#";
	private static final int ATTEMPTS = 5;
	
	/**
	 * 为SCH_CODE构建出实际数据的访问地址 */
	public static List<String> generateUrls(int schCode) {
		List<String> urls = new ArrayList<String>();
		for(int stuProvCode : stuProvCodes) {
			for(int category : categorys) {
				for(int batch : batchs) {
					urls.add(String.format(SCH_SCORE_URL, schCode, stuProvCode, category, batch));
				}
			}
		}
		return urls;
	}
	
	/**
	 * 从汇总页面解析出实际数据的访问地址 */
	public static List<String> parseUrls(int schCode) {
		String url = String.format(SCH_SCORE_URL_SUMMARY, schCode);
		System.out.println("读取" + url);
		List<String> urls = new ArrayList<String>();
		int count = 0;
		while(count++ < ATTEMPTS) {
			try {
				Document doc = Jsoup.connect(url).get();
				Elements aList = doc.select(".S_result table tr a");
				if (aList != null && aList.size() > 0) {
					for(Element a : aList) {
						String href = a.attr("href");
						for(int batch : batchs) {
							urls.add(String.format(SCH_SCORE_URL_LITE, href.substring(0, href.lastIndexOf("/") + 1), batch));
						}
					}
				}
				break;
			} catch (IOException e) {
				try {
					Thread.sleep((long) (Math.random() * 2000));
				} catch (InterruptedException e1) {}
			}
		}
		return urls;
	}
	
	/**
	 * 从URL中读取实际数据
	 * @param year 年份，0表示全部年份 */
	public static List<String> parseSqls(String url, int provCode, int schCode, int year) {
		List<String> sqls = new ArrayList<String>();
		int count = 0;
		while(count++ < ATTEMPTS) {
			try {
				Document doc = Jsoup.connect(url).get();
				Elements trList = doc.select(".S_result table tr");
				if (trList != null && trList.size() > 0) {
					for(int j = 1; j < trList.size(); j++) {
						Element tr = trList.get(j);
						if(tr.childNodeSize() > 2) { // 有效数据
							int stuProvCode = stuProvCode(url);
							try {
								Elements tdList = tr.getElementsByTag("td");
								if(year > 0) {
									int yearInWeb = Integer.parseInt(tdList.get(0).html().trim());
									if(yearInWeb == year) { // 只抓取当年数据
										sqls.add(parseSql(provCode, schCode, stuProvCode, tdList));
									}
								} else {
									sqls.add(parseSql(provCode, schCode, stuProvCode, tdList));
								}
							} catch (Exception e) {}
						}
					}
				}
				break;
			} catch (IOException e) {
				try {
					Thread.sleep((long) (Math.random() * 2000));
				} catch (InterruptedException e1) {}
			}
		}
		return sqls;
	}
	
	/**
	 * 从<td>元素中读取实际数据 */
	private static String parseSql(int provCode, int schCode, int stuProvCode, Elements tdList) {
		// 平均分
		String avgScoreStr = tdList.get(2).html().trim();
		int avgScore = Util.isNumberic(avgScoreStr) ? Integer.parseInt(avgScoreStr) : 0;
		// 最高分
		String highestScoreStr = tdList.get(1).html().trim();
		int highestScore = Util.isNumberic(highestScoreStr) ? Integer.parseInt(highestScoreStr) : 0;
		// 省控线
		String lowestScoreStr = tdList.get(3).html().trim();
		int lowestScore = Util.isNumberic(lowestScoreStr) ? Integer.parseInt(lowestScoreStr) : 0;
		return sql(provCode, schCode, stuProvCode, 
				tdList.get(4).html().trim(), // 考生类别
				tdList.get(5).html().trim(), // 录取批次
				Integer.parseInt(tdList.get(0).html().trim()), // 年份
				avgScore, highestScore, lowestScore);
	}
	
	/**
	 * 从URL中提取STU_PROV_CODE */
	private static int stuProvCode(String url) {
		url = url.substring(0, url.lastIndexOf("/"));
		url = url.substring(0, url.lastIndexOf("/"));
		url = url.substring(url.lastIndexOf("/") + 1);
		return Integer.parseInt(url);
	}
	
	/**
	 * 组装SCH_SCORE的SQL语句 */
	private static String sql(int provCode, int schCode, int stuProvCode, 
			String category, String batch, int year, 
			int avgScore, int highestScore, int lowestScore) {
		return String
				.format("INSERT INTO SCH_SCORE_%d(SCH_CODE,STU_PROV_CODE,CATEGORY,BATCH,YEAR,AVG_SCORE,HIGHEST_SCORE,LOWEST_SCORE) VALUES(%d,%d,'%s','%s',%d,%d,%d,%d)",
						provCode, schCode, stuProvCode, 
						category, batch, year, avgScore, highestScore, lowestScore);
	}

}
