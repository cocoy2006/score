package molab.java.multi;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import molab.java.Jdbc;
import molab.java.Status;
import molab.java.util.Util;

/**
 * @author ykk
 * 因完全遍历存在大量的无效链接，因此采用两个阶段遍历以提高数据获取效率。
 * 阶段1先从汇总页面获取有效数据链接，再遍历有效数据链接获取有效数据，同时将未获取到数据的学校编码记录日志，以便阶段2使用。
 * 阶段2从日志读取学校编码，采用完全遍历的方式获取数据。
 * 两个阶段都以省为单位，同时起62个线程分别获取省录取线和专业录取线。
 */
public class MultiMain {

	private static Jdbc jdbc;
	private static Set<Integer> schLogCodeSet; // 记录阶段1，对应sch.log+sch_err.log
	private static Set<Integer> schErrLogCodeSet; // 记录阶段1的异常数据，对应sch_err.log
	private static Set<Integer> schAgLogCodeSet; // 记录阶段2，对应sch_ag.log+sch_ag_err.log
	private static Set<Integer> schAgErrLogCodeSet; // 记录阶段2的异常数据，对应sch_ag_err.log
	private static Set<Integer> speLogCodeSet; // 记录阶段1，对应spe.log+spe_err.log
	private static Set<Integer> speErrLogCodeSet; // 记录阶段1的异常数据，对应spe_err.log
	private static Set<Integer> speAgLogCodeSet; // 记录阶段2，对应spe_ag.log+spe_ag_err.log
	private static Set<Integer> speAgErrLogCodeSet; // 记录阶段2的异常数据，对应spe_ag_err.log
	
	public static void main(String[] args) throws SQLException, InterruptedException, IOException {
		if(args.length < 3) {
			throw new IllegalArgumentException("Arguments: <host> <username> <password>");
		}
		// 打开mysql连接
		jdbc = new Jdbc(args[0], args[1], args[2]);
		// 初始化日志文件
		initPhase(Status.Phase.TWO.getInt());
		if(schAgLogCodeSet.size() > 0 || speAgLogCodeSet.size() > 0) {
			phaseTwo(args);
		} else {
			initPhase(Status.Phase.ONE.getInt());
			phaseOne(args);
			initPhase(Status.Phase.TWO.getInt());
			phaseTwo(args);
		}
		
		System.out.println("Done.");
		System.exit(0);
	}
	
	private static void initPhase(int phase) throws IOException, SQLException {
		if(phase == Status.Phase.ONE.getInt()) {
			// 省录取线
			schErrLogCodeSet = Util.build(Util.SCH_ERR_LOG);
			schLogCodeSet = Util.build(Util.SCH_LOG);
			schLogCodeSet.addAll(schErrLogCodeSet);
			// 专业录取线
			speErrLogCodeSet = Util.build(Util.SPE_ERR_LOG);
			speLogCodeSet = Util.build(Util.SPE_LOG);
			speLogCodeSet.addAll(speErrLogCodeSet);
		} else {
			// 省录取线
			schAgErrLogCodeSet = Util.build(Util.SCH_AG_ERR_LOG);
			schAgLogCodeSet = Util.build(Util.SCH_AG_LOG);
			schAgLogCodeSet.addAll(schAgErrLogCodeSet);
			// 专业录取线
			speAgErrLogCodeSet = Util.build(Util.SPE_AG_ERR_LOG);
			speAgLogCodeSet = Util.build(Util.SPE_AG_LOG);
			speAgLogCodeSet.addAll(speAgErrLogCodeSet);
		}
	}
	
	private static Map<Integer, ArrayList<Integer>> initMap() {
		Map<Integer, ArrayList<Integer>> map = new HashMap<Integer, ArrayList<Integer>>();
		for(int i = 10000; i <= 10031; i++) {
			if(i != 10020) {
				map.put(i, new ArrayList<Integer>());
			}
		}
		return map;
	}
	
	/**
	 * 从汇总页面开始读取数据 
	 * @throws InterruptedException */
	private static void phaseOne(String[] args) throws InterruptedException {
		Map<Integer, ArrayList<Integer>> schoolMap = initMap();
		jdbc.getSchoolMap(schoolMap);
		// 线程池
		CountDownLatch countDown = new CountDownLatch(62);
		// 抓取省录取线
		fetchSchScore(args, countDown, schoolMap, schLogCodeSet, Status.Phase.ONE.getInt());
		// 抓取专业录取线
		fetchSpeScore(args, countDown, schoolMap, speLogCodeSet, Status.Phase.ONE.getInt());
		// 等待任务结束
		countDown.await();
	}
	
	/**
	 * 从全覆盖页面开始读取数据（防止遗漏的检测方法） 
	 * @throws IOException 
	 * @throws InterruptedException */
	private static void phaseTwo(String[] args) throws IOException, InterruptedException {
		// 加载学校数据
		String schCodes = Util.read(Util.SCH_ERR_LOG).trim();
		if(schCodes.endsWith(",")) {
			schCodes += "0";
		}
		Map<Integer, ArrayList<Integer>> schSchoolMap = initMap();
		jdbc.getSchoolMap(schSchoolMap, schCodes);
		
		schCodes = Util.read(Util.SPE_ERR_LOG).trim();
		if(schCodes.endsWith(",")) {
			schCodes += "0";
		}
		Map<Integer, ArrayList<Integer>> speSchoolMap = initMap();
		jdbc.getSchoolMap(speSchoolMap, schCodes);
		// 线程池
		CountDownLatch countDown = new CountDownLatch(62);
		// 抓取省录取线
		fetchSchScore(args, countDown, schSchoolMap, schAgLogCodeSet, Status.Phase.TWO.getInt());
		// 抓取专业录取线
		fetchSpeScore(args, countDown, speSchoolMap, speAgLogCodeSet, Status.Phase.TWO.getInt());
		// 等待任务结束
		countDown.await();
	}
	
	private static void fetchSchScore(String[] args, CountDownLatch countDown, 
			Map<Integer, ArrayList<Integer>> schoolMap, Set<Integer> logCodeSet, int phase) throws InterruptedException {
		for(int provCode : schoolMap.keySet()) {
			try {
				Thread.sleep((long) (Math.random() * 1000));
			} catch (InterruptedException e1) {}
			new SchScoreMultiThread(args, countDown, schoolMap.get(provCode), logCodeSet, provCode, phase).start();
		}
	}
	
	private static void fetchSpeScore(String[] args, CountDownLatch countDown,
			Map<Integer, ArrayList<Integer>> schoolMap, Set<Integer> logCodeSet, int phase) throws InterruptedException {
		for(int provCode : schoolMap.keySet()) {
			try {
				Thread.sleep((long) (Math.random() * 1000));
			} catch (InterruptedException e1) {}
			new SpeScoreMultiThread(args, countDown, schoolMap.get(provCode), logCodeSet, provCode, phase).start();
		}
	}
	
}
