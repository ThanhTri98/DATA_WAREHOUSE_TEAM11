package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import modal.Configuration;
import util.ConnectionDB;

public class ControlDB {
	private String CONTROL_DB_NAME;
	private String CONTROL_DB_USER;
	private String CONTROL_DB_PASS;
	Connection connection = null;
	PreparedStatement pst = null;
	ResultSet rs = null;
	String sql;

	public ControlDB() {
		this.CONTROL_DB_NAME = "jdbc:mysql://localhost:3306/controldb";
		this.CONTROL_DB_USER = "root";
		this.CONTROL_DB_PASS = "";
	}

	public Configuration getConfig(int id_config) {
		try {
			sql = "SELECT * FROM CONFIGURATION WHERE CONFIG_ID=?";
			connection = ConnectionDB.createConnection(CONTROL_DB_NAME, CONTROL_DB_USER, CONTROL_DB_PASS);
			pst = connection.prepareStatement(sql);
			pst.setInt(1, id_config);
			rs = pst.executeQuery();
			if (rs.next()) {
				return new Configuration().getConfiguration(rs);
			}
			return null;
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

	public ResultSet selectAllField(String db_name, String user_name, String password, String table_name,
			String condition_name, String condition_value, String isInteger) {
		boolean isLastIndex = false;
		ResultSet rs_allField = null;
		try {
			connection = ConnectionDB.createConnection(db_name, user_name, password);
			if (condition_name == null) {
				sql = "SELECT * FROM " + table_name;
				pst = connection.prepareStatement(sql);
			} else {
				sql = "SELECT * FROM " + table_name + " WHERE ";
				String[] con_name = condition_name.split(",");
				String[] con_value = condition_value.split(",");
				String[] isInt = isInteger.split(",");
				for (int i = 0; i < con_name.length; i++) {
					if (!isLastIndex) {
						if (i == 0) {
							sql += con_name[i] + "=?";
						} else {
							sql += " AND " + con_name[i] + "=?";
						}
						if (i == con_name.length - 1) {
							pst = connection.prepareStatement(sql);
							isLastIndex = true;
							i = -1;
						} else {
							continue;
						}
					} else {
						if (isInt[i].equals("true")) {
							pst.setInt(i + 1, Integer.parseInt(con_value[i]));
						} else {
							pst.setString(i + 1, con_value[i]);
						}
					}
				}

			}
			rs_allField = pst.executeQuery();
			return rs_allField;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public boolean insertValues(String db_name, String user_name, String password, String table_name,
			String column_list, String values) throws SQLException {

		sql = "INSERT INTO " + table_name + "(" + column_list + ") VALUES " + values;
		connection = ConnectionDB.createConnection(db_name, user_name, password);
		pst = connection.prepareStatement(sql);
		int result = pst.executeUpdate();
		try {
			if (connection != null)
				connection.close();
			if (pst != null)
				pst.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result > 0;

	}

	public ResultSet selectOneField(String db_name, String user_name, String password, String table_name, String field,
			String condition_name, String condition_value, boolean isInteger) {
		try {
			connection = ConnectionDB.createConnection(db_name, user_name, password);
			if (condition_name == null) {
				sql = "SELECT " + field + " FROM " + table_name;
				pst = connection.prepareStatement(sql);
			} else {
				sql = "SELECT " + field + " FROM " + table_name + " WHERE " + condition_name + "=?";
				if (isInteger) {
					pst.setInt(1, Integer.parseInt(condition_value));
				} else {
					pst.setString(1, condition_value);
				}
				pst = connection.prepareStatement(sql);
			}
			rs = pst.executeQuery();
			return rs;
		} catch (Exception e) {
			return null;
		}
	}

	public boolean truncateTable(String db_name, String user, String pass, String table_name) {
		try {
			sql = "TRUNCATE " + table_name;
			connection = ConnectionDB.createConnection(db_name, user, pass);
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
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	public boolean updateLogs(String db_name, String user_name, String password, int id_logs, String name_field,
			String value, boolean isInteger) {

		sql = "UPDATE LOGS SET " + name_field + "=?,FILE_TIMESTAMP=NOW() WHERE ID=?";
		try {
			connection = ConnectionDB.createConnection(db_name, user_name, password);
			pst = connection.prepareStatement(sql);
			if (isInteger) {
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
	public String getCONTROL_DB_NAME() {
		return CONTROL_DB_NAME;
	}

	public String getCONTROL_DB_USER() {
		return CONTROL_DB_USER;
	}

	public String getCONTROL_DB_PASS() {
		return CONTROL_DB_PASS;
	}
}
