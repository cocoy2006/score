package test.java;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import molab.java.Jdbc;
import molab.java.util.Util;


public class AgMain {

	private static Jdbc jdbc;
	private static Map<Integer, Integer> schoolMap; // <SCH_CODE, PROV_CODE>
	private static List<Integer> stuProvCodes; // 生源地编号
	private static final Integer[] categorys = {10035, 10034, 10090, 10091, 10093}; // 考生类别编号
	private static final Integer[] batchs = {10036, 10037, 10038, 10148, 10049}; // 录取批次编号
	private static final Integer[] years = {2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015}; // （专业录取线的）年份
	private static final String SCH_SCORE_URL = "http://gkcx.eol.cn/schoolhtm/schoolAreaPoint/%d/%d/%d/%d.htm#";
	private static final String SPE_SCORE_URL = "http://gkcx.eol.cn/schoolhtm/specialty/%d/%d/specialtyScoreDetail_%d_%d.htm#";
	private static final int ATTEMPTS = 5;
	
	public static void main(String[] args) throws SQLException, InterruptedException, IOException {
		init();
		List<String> sqls = new ArrayList<String>();
		for(int schCode : schoolMap.keySet()) {
			System.out.println("正在处理" + schCode + "(学校编号)");
			int provCode = schoolMap.get(schCode);
			// 抓取省录取线
			fetchSchScore(sqls, provCode, schCode);
			// 抓取专业录取线
			fetchSpeScore(sqls, provCode, schCode);
			// 批量提交数据
			if(sqls.size() > 0) {
				System.out.println(schCode + "(学校编号)找到" + sqls.size() + "条记录.");
				jdbc.batchUpdate(sqls);
				sqls.clear();
			} else {
				System.err.println(schCode + "(学校编号)没有记录.");
			}
			// 随机等待2s以内，防止对端系统屏蔽
			try {
				Thread.sleep((long) (Math.random() * 2000));
			} catch (InterruptedException e) {}
		}
		
		System.out.println("Done.");
		System.exit(0);
	}
	
	private static void init() throws SQLException, IOException {
		// 打开mysql连接
		jdbc = new Jdbc("localhost", "root", "molab,123");
		// 初始化学校数据表
		schoolMap = new LinkedHashMap<Integer, Integer>();
		jdbc.getCodes(schoolMap, "59,82,95,653,1584");
		// 初始化生源省数据表
		stuProvCodes = new ArrayList<Integer>();
		for(int i = 10000; i <= 10031; i++) {
			if(i != 10020) {
				stuProvCodes.add(i);
			}
		}
	}

	private static void fetchSchScore(List<String> sqls, int provCode, int schCode) throws InterruptedException, IOException {
		for(int stuProvCode : stuProvCodes) {
			for(int category : categorys) {
				for(int batch : batchs) {
					Thread.sleep((long) (Math.random() * 100));
					String url = String.format(SCH_SCORE_URL, schCode, stuProvCode, category, batch);
					int count = 0;
					while(count++ < ATTEMPTS) {
						try {
							Document doc = Jsoup.connect(url).get();
							Elements trList = doc.select(".S_result table tr");
							if (trList != null && trList.size() > 0) {
								for(int i = 1; i < trList.size(); i++) {
									Element tr = trList.get(i);
									if(tr.childNodeSize() > 2) { // 有效数据
										Elements tdList = tr.getElementsByTag("td");
										// 平均分
										String avgScoreStr = tdList.get(2).html().trim();
										int avgScore = Util.isNumberic(avgScoreStr) ? Integer.parseInt(avgScoreStr) : 0;
										// 最高分
										String highestScoreStr = tdList.get(1).html().trim();
										int highestScore = Util.isNumberic(highestScoreStr) ? Integer.parseInt(highestScoreStr) : 0;
										// 省控线
										String lowestScoreStr = tdList.get(3).html().trim();
										int lowestScore = Util.isNumberic(lowestScoreStr) ? Integer.parseInt(lowestScoreStr) : 0;
										sqls.add(schScoreSql(provCode, schCode, stuProvCode, 
												tdList.get(4).html().trim(), // 考生类别
												tdList.get(5).html().trim(), // 录取批次
												Integer.parseInt(tdList.get(0).html().trim()), // 年份
												avgScore, highestScore, lowestScore));
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
				}
			}
		}
	}
	
	private static String schScoreSql(int provCode, int schCode, int stuProvCode, 
			String category, String batch, int year, 
			int avgScore, int highestScore, int lowestScore) {
		return String
				.format("INSERT INTO SCH_SCORE_%s(SCH_CODE,STU_PROV_CODE,CATEGORY,BATCH,YEAR,AVG_SCORE,HIGHEST_SCORE,LOWEST_SCORE) VALUES(%d,%d,%s,%s,%d,%d,%d,%d)",
						String.valueOf(provCode), schCode, stuProvCode, 
						category, batch, year, avgScore, highestScore, lowestScore);
	}
	
	private static void fetchSpeScore(List<String> sqls, int provCode, int schCode) throws InterruptedException, IOException {
		for(int stuProvCode : stuProvCodes) {
			for(int category : categorys) {
				for(int year : years) {
					Thread.sleep((long) (Math.random() * 100));
					String url = String.format(SPE_SCORE_URL, schCode, category, year, stuProvCode);
					int count = 0;
					while(count++ < ATTEMPTS) {
						try {
							Document doc = Jsoup.connect(url).get();
							Elements trList = doc.select(".S_result table tr");
							if (trList != null && trList.size() > 0) {
								for(int i = 1; i < trList.size(); i++) {
									Element tr = trList.get(i);
									if(tr.childNodeSize() > 2) { // 有效数据
										Elements tdList = tr.getElementsByTag("td");
										// 平均分
										String avgScoreStr = tdList.get(2).html().trim();
										int avgScore = Util.isNumberic(avgScoreStr) ? Integer.parseInt(avgScoreStr) : 0;
										// 最高分
										String highestScoreStr = tdList.get(3).html().trim();
										int highestScore = Util.isNumberic(highestScoreStr) ? Integer.parseInt(highestScoreStr) : 0;
										// 最低分
										String lowestScoreStr = tdList.get(4).html().trim();
										int lowestScore = Util.isNumberic(lowestScoreStr) ? Integer.parseInt(lowestScoreStr) : 0;
										sqls.add(speScoreSql(provCode, schCode, 
												tdList.get(0).html().trim(), // 专业名称
												stuProvCode, 
												tdList.get(5).html().trim(), // 考生类别
												tdList.get(6).html().trim(), // 录取批次
												Integer.parseInt(tdList.get(1).html().trim()), // 年份
												avgScore, highestScore, lowestScore));
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
				}
			}
		}
	}
	
	private static String speScoreSql(int provCode, int schCode, 
			String speciality, int stuProvCode, 
			String category, String batch, int year, 
			int avgScore, int highestScore, int lowestScore) {
		return String
				.format("INSERT INTO SPE_SCORE_%s(SCH_CODE,SPECIALITY,STU_PROV_CODE,CATEGORY,BATCH,YEAR,AVG_SCORE,HIGHEST_SCORE,LOWEST_SCORE) VALUES(%d,%s,%d,%s,%s,%d,%d,%d,%d)",
						String.valueOf(provCode), schCode, speciality, stuProvCode, 
						category, batch, year, avgScore, highestScore, lowestScore);
	}
	
}
