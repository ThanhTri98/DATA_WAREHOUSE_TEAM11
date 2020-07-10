package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import modal.Configuration;
import modal.Student;
import util.ConnectionDB;

public class ControlDB {
	static Connection connection = null;
	public static final String CONTROL_DB_NAME = "jdbc:mysql://localhost:3306/controldb";

	public static final String CONTROL_DB_USER = "root";
	public static final String CONTROL_DB_PASS = "";
	static PreparedStatement pst = null;
	static ResultSet rs = null;
	static String sql;

	public static Configuration getConfig() {
		Configuration conf = null;
		try {
			conf = new Configuration();
			sql = "SELECT * FROM CONFIGURATION";
			connection = ConnectionDB.createConnection(CONTROL_DB_NAME, CONTROL_DB_USER, CONTROL_DB_PASS);
			pst = connection.prepareStatement(sql);
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
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static ResultSet selectAllField(String db_name, String user_name, String password, String table_name,
			String condition_name, String condition_value) {
		ResultSet rs = null;
		PreparedStatement pst = null;
		try {
			connection = ConnectionDB.createConnection(db_name, user_name, password);
			if (condition_name == null) {
				sql = "SELECT * FROM " + table_name;
				pst = connection.prepareStatement(sql);
			} else {
				sql = "SELECT * FROM " + table_name + " WHERE " + condition_name + "=?";
				pst = connection.prepareStatement(sql);
				pst.setString(1, condition_value);
			}
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
		connection = ConnectionDB.createConnection(db_name, user_name, password);
		pst = connection.prepareStatement(sql);
		int result = pst.executeUpdate();
		try {
			if (rs != null)
				rs.close();
			if (connection != null)
				connection.close();
			if (pst != null)
				pst.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result > 0;

	}

	public static boolean insertValuesDBStagingToDBWareHouse(String db_name, String user_name, String password,
			String table_name, String column_list, Student stu, int id_log) {
		try {
			sql = "INSERT INTO " + table_name + "(" + column_list + ") VALUES " + "(?,?,?,?,?,?,?,?,?,?,?,?,NOW())";
			connection = ConnectionDB.createConnection(db_name, user_name, password);
			pst = connection.prepareStatement(sql);
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
			pst.setInt(12, id_log);
			int result = pst.executeUpdate();
			return result > 0;
		} catch (SQLException e1) {
			e1.printStackTrace();
			return false;
		} finally {
			try {
				if (pst != null)
					pst.close();
				if (rs != null)
					rs.close();
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	public static ResultSet selectOneField(String db_name, String user_name, String password, String table_name,
			String field, String condition_name, String condition_value) {
		try {
			connection = ConnectionDB.createConnection(db_name, user_name, password);
			if (condition_name == null) {
				sql = "SELECT " + field + " FROM " + table_name;
				pst = connection.prepareStatement(sql);
			} else {
				sql = "SELECT " + field + " FROM " + table_name + " WHERE " + condition_name + "=?";
				pst.setString(1, condition_value);
				pst = connection.prepareStatement(sql);
			}
			rs = pst.executeQuery();
			return rs;
		} catch (Exception e) {
			return null;
		}
	}

	public static boolean truncateTable(String controlDbNameStaging, String controlDbUser, String controlDbPass,
			String table_name) {
		try {
			sql = "TRUNCATE " + table_name;
			connection = ConnectionDB.createConnection(controlDbNameStaging, controlDbUser, controlDbPass);
			pst = connection.prepareStatement(sql);
			int result = pst.executeUpdate();
			return result > 0;
		} catch (SQLException e1) {
			e1.printStackTrace();
			return false;
		} finally {
			try {
				if (pst != null)
					pst.close();
				if (rs != null)
					rs.close();
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	public static boolean updateLogs(String db_name, String user_name, String password, int id_logs, String name_field,
			String value) {

		sql = "UPDATE LOGS SET " + name_field + "=?,FILE_TIMESTAMP=NOW() WHERE ID=?";
		try {
			connection = ConnectionDB.createConnection(db_name, user_name, password);
			pst = connection.prepareStatement(sql);
			if (name_field.equals("staging_load_count") || name_field.equals("warehouse_load_count")) {
				pst.setInt(1, Integer.parseInt(value));
			} else {
				pst.setString(1, value);
			}
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
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

//	public boolean tableExist(String table_name) {
//	try {
//		DatabaseMetaData dbm = ConnectionDB.createConnection(this.target_db_name).getMetaData();
//		ResultSet tables = dbm.getTables(null, null, table_name, null);
//		try {
//			if (tables.next()) {
//				return true;
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//			return false;
//		}
//	} catch (SQLException e) {
//		e.printStackTrace();
//		return false;
//	}
//
//	return false;
//}

//	public boolean createTable(String table_name, String variables, String column_list) {
//		sql = "CREATE TABLE " + table_name + " (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,";
//		String[] vari = variables.split(",");
//		String[] col = column_list.split(",");
//		for (int i = 0; i < vari.length; i++) {
//			sql += col[i] + " " + vari[i] + " NOT NULL,";
//		}
//		sql = sql.substring(0, sql.length() - 1) + ")";
//		try {
//			pst = ConnectionDB.createConnection(this.target_db_name).prepareStatement(sql);
//			pst.executeUpdate();
//			return true;
//		} catch (SQLException e) {
//			e.printStackTrace();
//			return false;
//		} finally {
//			try {
//				if (pst != null)
//					pst.close();
//				if (rs != null)
//					rs.close();
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//
//		}
//	}
}
