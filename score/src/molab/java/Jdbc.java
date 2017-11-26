package molab.java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import test.java.Codes;

public class Jdbc {
	
	private static final Logger log = Logger.getLogger(Jdbc.class.getName());
	
	private Connection conn = null;
	private Statement stmt = null;
	
	public Jdbc(String host, String username, String password) {
		String url = "jdbc:mysql://" + host + ":3306/SCORE?useUnicode=true&amp;characterEncoding=UTF-8";
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(url, username, password);
			stmt = conn.createStatement();
		} catch (ClassNotFoundException e) {
			log.severe(e.getMessage());
		} catch (SQLException e) {
			log.severe(e.getMessage());
		}
		log.info("Database connection established suceessfully.");
	}
	
	public void getSchoolMap(Map<Integer, ArrayList<Integer>> schoolMap) {
		String sql = "SELECT PROV_CODE,SCH_CODE FROM SCHOOL ORDER BY SCH_CODE";
		try {
			ResultSet rs = stmt.executeQuery(sql);
			if(rs != null) {
				while(rs.next()) {
					schoolMap.get(rs.getInt("PROV_CODE")).add(rs.getInt("SCH_CODE"));
				}
			}
		} catch (SQLException e) {
			log.severe(e.getMessage());
		}
	}
	
	public void getSchoolMap(Map<Integer, ArrayList<Integer>> schoolMap, String schCodes) {
		String sql = String.format("SELECT PROV_CODE,SCH_CODE FROM SCHOOL WHERE SCH_CODE IN(%s) ORDER BY SCH_CODE", schCodes);
		try {
			ResultSet rs = stmt.executeQuery(sql);
			if(rs != null) {
				while(rs.next()) {
					schoolMap.get(rs.getInt("PROV_CODE")).add(rs.getInt("SCH_CODE"));
				}
			}
		} catch (SQLException e) {
			log.severe(e.getMessage());
		}
	}
	
	public Codes getSchoolMap(String schName) {
		String sql = String.format("SELECT PROV_CODE,SCHOOL,SCH_CODE FROM SCHOOL WHERE SCHOOL LIKE '%s'", schName);
		try {
			ResultSet rs = stmt.executeQuery(sql);
        	if(rs != null) {
        		Codes codes = new Codes();
    			while(rs.next()) {
    				codes.setProvCode(rs.getInt("PROV_CODE"));
    				codes.setSchool(rs.getString("SCHOOL"));
    				codes.setSchCode(rs.getInt("SCH_CODE"));
    				return codes;
    			}
        	}
		} catch (SQLException e) {
			log.severe(e.getMessage());
		}
		return null;
	}
	
	public void getCodes(Set<Integer> codeSet) {
		String sql = "SELECT SCH_CODE FROM SCHOOL";
		try {
			ResultSet rs = stmt.executeQuery(sql);
        	if(rs != null) {
    			while(rs.next()) {
    				codeSet.add(rs.getInt("SCH_CODE"));
    			}
        	}
		} catch (SQLException e) {
			log.severe(e.getMessage());
		}
	}
	
	public void getCodes(Map<Integer, Integer> schoolMap, String schCodes) {
		String sql = String.format("SELECT PROV_CODE,SCH_CODE FROM SCHOOL WHERE SCH_CODE IN(%s)", schCodes);
		try {
			ResultSet rs = stmt.executeQuery(sql);
        	if(rs != null) {
    			while(rs.next()) {
    				schoolMap.put(rs.getInt("SCH_CODE"), rs.getInt("PROV_CODE"));
    			}
        	}
		} catch (SQLException e) {
			log.severe(e.getMessage());
		}
	}
	
	public boolean getCount(String prefix, int provCode, int schCode) {
		String table = prefix + provCode;
		String sql = String.format("SELECT COUNT(*) FROM %s WHERE SCH_CODE=%d", table, schCode);
		try {
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				return rs.getInt(1) > 0;
			}
		} catch (SQLException e) {
			log.severe(e.getMessage());
		}
		return false;
	}
	
	public boolean getCount(String prefix, int provCode, int schCode, int year) {
		String table = prefix + provCode;
		String sql = String.format("SELECT COUNT(*) FROM %s WHERE SCH_CODE=%d AND YEAR=%d", table, schCode, year);
		try {
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				return rs.getInt(1) > 0;
			}
		} catch (SQLException e) {
			log.severe(e.getMessage());
		}
		return false;
	}
	
	public int[] batchUpdate(List<String> sqls) {
		try {
			if(sqls.size() > 0) {
				conn.setAutoCommit(false);
				for(String sql : sqls) {
					stmt.addBatch(sql);
				}
				return stmt.executeBatch();
			}
		} catch (SQLException e) {
			log.severe(e.getMessage());
		} finally {
			try {
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				log.severe(e.getMessage());
			}
		}
		return null;
	}
	
	public void close() throws SQLException {
		if(stmt != null) {
			stmt.close();
		}
		if(conn != null) {
			conn.close();
		}
	}

}
