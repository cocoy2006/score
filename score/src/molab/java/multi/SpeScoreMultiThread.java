package molab.java.multi;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import molab.java.Jdbc;
import molab.java.Status;
import molab.java.util.SpeUtil;
import molab.java.util.Util;

public class SpeScoreMultiThread extends Thread {

	private String[] args;
	private Jdbc jdbc;
	private ArrayList<Integer> schCodeList;
	private Set<Integer> logSchCodeSet;
	private CountDownLatch countDown;
	private int provCode;
	private int phase;
	private final String TABLE_PREFIX = "SPE_SCORE_";
	
	public SpeScoreMultiThread(String[] args, CountDownLatch countDown, 
			ArrayList<Integer> schCodeList, Set<Integer> logSchCodeSet, 
			int provCode, int phase) {
		this.args = args;
		this.countDown = countDown;
		this.schCodeList = schCodeList;
		this.logSchCodeSet = logSchCodeSet;
		this.provCode = provCode;
		this.phase = phase;
	}
	
	@Override
	public void run() {
		jdbc = new Jdbc(args[0], args[1], args[2]);
		String url = null;
		List<String> sqls = new ArrayList<String>();
		for(int schCode : schCodeList) {
			if(jdbc.getCount(TABLE_PREFIX, provCode, schCode) 
					|| logSchCodeSet.contains(schCode)) {
				System.out.println(schCode + "(学校编号)已存在记录.");
				continue;
			}
			// 抓取数据
			List<String> urls = null;
			if(phase == Status.Phase.ONE.getInt()) {
				urls = SpeUtil.parseUrls(schCode);
			} else {
				urls = SpeUtil.generateUrls(schCode);
			}
			// 抓取实际数据
			if(urls.size() > 0) {
				for(int i = 0; i < urls.size(); i++) {
					url = urls.get(i);
					try {
						Thread.sleep((long) (Math.random() * 100));
					} catch (InterruptedException e) {}
					sqls.addAll(SpeUtil.parseSqls(url, provCode, schCode));
				}
			}
			// 批量提交数据
			if(sqls.size() > 0) {
				System.out.println(schCode + "(学校编号)找到" + sqls.size() + "条记录.");
				jdbc.batchUpdate(sqls);
				sqls.clear();
				if(phase == Status.Phase.ONE.getInt()) {
					Util.log(Util.SPE_LOG, schCode);
				} else {
					Util.log(Util.SPE_AG_LOG, schCode);
				}
			} else {
				System.err.println(schCode + "(学校编号)没有记录.");
				if(phase == Status.Phase.ONE.getInt()) {
					Util.log(Util.SPE_ERR_LOG, schCode);
				} else {
					Util.log(Util.SPE_AG_ERR_LOG, schCode);
				}
			}
			// 随机等待2s以内，防止对端系统屏蔽
			try {
				Thread.sleep((long) (Math.random() * 2000));
			} catch (InterruptedException e) {}
		}
		countDown.countDown();
		try {
			jdbc.close();
		} catch (SQLException e) {}
	}
	
}
