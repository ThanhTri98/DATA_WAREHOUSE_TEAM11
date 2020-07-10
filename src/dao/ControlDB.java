package dao;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import modal.Configuration;
import modal.Student;
import util.ConnectionDB;

public class ControlDB {
	public static final String DATETIME_FORMAT = "yyyy/MM/dd HH:mm:ss";
	public static DateTimeFormatter dtf = DateTimeFormatter.ofPattern(DATETIME_FORMAT);
	public static LocalDateTime now = LocalDateTime.now();

	public static final String CONTROL_DB_NAME = "jdbc:mysql://localhost:3306/controldb";
	public static final String CONTROL_DB_NAME_STAGING = "jdbc:mysql://localhost:3306/db_staging";
	public static final String CONTROL_DB_NAME_WAREHOUSE = "jdbc:mysql://localhost:3306/warehouse";
	public static final String CONTROL_DB_USER = "root";
	public static final String CONTROL_DB_PASS = "123456";
	static PreparedStatement pst = null;
	static ResultSet rs = null;
	static String sql;

	public static Configuration getConfig() {
		Configuration conf = null;
		try {
			conf = new Configuration();
			sql = "SELECT * FROM CONFIGURATION";
			pst = ConnectionDB.createConnection(CONTROL_DB_NAME, CONTROL_DB_USER, CONTROL_DB_PASS)
					.prepareStatement(sql);
			rs = pst.executeQuery();
			conf = conf.getConfiguration(rs);
			return conf;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				if (pst != null)
					pst.close();
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static ResultSet selectAllField(String db_name, String user_name, String password, String table_name) {
		ResultSet rs = null;
		PreparedStatement pst = null;
		try {
			sql = "SELECT * FROM " + table_name;
			pst = ConnectionDB.createConnection(db_name, user_name, password).prepareStatement(sql);
			rs = pst.executeQuery();
			return rs;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}

	}

	public static boolean insertValues(String db_name, String user_name, String password, String table_name,
			String column_list, String values) throws SQLException {
		sql = "INSERT INTO " + table_name + "(" + column_list + ") VALUES " + values;
		pst = ConnectionDB.createConnection(db_name, user_name, password).prepareStatement(sql);
		int result = pst.executeUpdate();
		try {
			if (pst != null)
				pst.close();
			if (rs != null)
				rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result > 0;
	}

	public static boolean insertValuesDBStagingToDBWareHouse(String db_name, String user_name, String password,
			String table_name, String column_list, Student stu) throws SQLException, Exception {
		sql = "INSERT INTO " + table_name + "(" + column_list + ") VALUES " + "(?,?,?,?,?,?,?,?,?,?,?)";
		pst = ConnectionDB.createConnection(db_name, user_name, password).prepareStatement(sql);
		pst.setInt(1, stu.getStt());
		pst.setString(2, stu.getMssv());
		pst.setString(3, stu.getHo());
		pst.setString(4, stu.getTen());
		pst.setString(5, stu.getNgaySinh());
		pst.setString(6, stu.getMaLop());
		pst.setString(7, stu.getTenLop());
		pst.setString(8, stu.getSdt());
		pst.setString(9, stu.getEmail());
		pst.setString(10, stu.getQueQuan());
		pst.setString(11, stu.getGhiChu());
		int result = pst.executeUpdate();
		try {
			if (pst != null)
				pst.close();
			if (rs != null)
				rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result > 0;
	}

	public static boolean truncateTable(String controlDbNameStaging, String controlDbUser, String controlDbPass,
			String table_name) throws SQLException {
		sql = "TRUNCATE " + table_name;
		pst = ConnectionDB.createConnection(controlDbNameStaging, controlDbUser, controlDbPass).prepareStatement(sql);
		int result = pst.executeUpdate();
		try {
			if (pst != null)
				pst.close();
			if (rs != null)
				rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result > 0;
	}

	public static String selectOneField(String db_name, String user_name, String password, String table_name,
			String field, String conditon_name, String conditon_value) {
		try {
			sql = "SELECT " + field + " FROM " + table_name + " WHERE " + conditon_name + "=?";
			pst = ConnectionDB.createConnection(db_name, user_name, password).prepareStatement(sql);
			pst.setString(1, conditon_value);
			rs = pst.executeQuery();
			rs.next();
			return rs.getString(field);
		} catch (Exception e) {
			return null;
		} finally {
			try {
				if (pst != null)
					pst.close();
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static boolean updateLogs(String db_name, String user_name, String password, int id_logs,
			String file_status) {
		sql = "UPDATE LOGS SET FILE_STATUS=?,FILE_TIMESTAMP=? WHERE ID=?";
		try {
			pst = ConnectionDB.createConnection(db_name, user_name, password).prepareStatement(sql);
			pst.setString(1, file_status);
			pst.setString(2, dtf.format(now));
			pst.setInt(3, id_logs);
			return pst.executeUpdate() > 0;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				if (pst != null)
					pst.close();
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
//
		}
	}

	public static boolean updateFileStatus(String db_name, String user_name, String password, int id_logs,
			String file_status) {
		sql = "UPDATE LOGS SET FILE_STATUS=?,FILE_TIMESTAMP=NOW() WHERE ID=?";
		try {
			pst = ConnectionDB.createConnection(db_name, user_name, password).prepareStatement(sql);
			pst.setString(1, file_status);
			pst.setInt(2, id_logs);
			return pst.executeUpdate() > 0;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				if (pst != null)
					pst.close();
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static boolean updateCountLines(String db_name, String user_name, String password, int id_logs, int count) {
		sql = "UPDATE LOGS SET STAGING_LOAD_COUNT=?,FILE_TIMESTAMP=NOW() WHERE ID=?";
		try {
			pst = ConnectionDB.createConnection(db_name, user_name, password).prepareStatement(sql);
			pst.setInt(1, count);
//			pst.setString(2, dtf.format(now));
			pst.setInt(2, id_logs);
			return pst.executeUpdate() > 0;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				if (pst != null)
					pst.close();
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
