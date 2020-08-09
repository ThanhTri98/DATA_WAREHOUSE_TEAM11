package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import modal.Configuration;
import modal.MailConfig;
import modal.Process;
import modal.SCP;
import util.ConnectionDB;

public class ControlDB {
	private String CONTROL_DB_NAME;
	private String CONTROL_DB_USER;
	private String CONTROL_DB_PASS;
	Connection connection = null;
	PreparedStatement pst = null;
	String sql;

	public ControlDB() {
		this.CONTROL_DB_NAME = "jdbc:mysql://localhost:3306/controldb";
		this.CONTROL_DB_USER = "root";
		this.CONTROL_DB_PASS = "";
	}

	public List<modal.Process> getListProcess() {
		ResultSet rs_process = null;
		try {
			List<modal.Process> listProcess = new ArrayList<modal.Process>();
			rs_process = selectAllField(CONTROL_DB_NAME, CONTROL_DB_USER, CONTROL_DB_PASS, "PROCESS", null, null, null);
			while (rs_process.next()) {
				listProcess.add(new Process().getProcess(rs_process));
			}
			return listProcess.size() != 0 ? listProcess : null;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				if (rs_process != null)
					rs_process.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public Configuration getConfig(int id_config) {
		ResultSet rs_config = null;
		try {
			rs_config = selectAllField(CONTROL_DB_NAME, CONTROL_DB_USER, CONTROL_DB_PASS, "CONFIGURATION", "CONFIG_ID",
					id_config + "", "true");
			if (rs_config.isClosed())
				System.out.println("dong");
			if (rs_config.next()) {
				return new Configuration().getConfiguration(rs_config);
			}
			return null;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				if (rs_config != null)
					rs_config.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public SCP getSCP(int id_config) {
		ResultSet rs_scp = null;
		try {
			rs_scp = selectAllField(CONTROL_DB_NAME, CONTROL_DB_USER, CONTROL_DB_PASS, "SCP", "CONFIG_ID",
					id_config + "", "true");
			if (rs_scp.next()) {
				return new SCP().getSCP(rs_scp);
			}
			return null;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				if (rs_scp != null)
					rs_scp.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public MailConfig getMailConfig() {
		ResultSet rs_mail = null;
		try {
			rs_mail = selectAllField(CONTROL_DB_NAME, CONTROL_DB_USER, CONTROL_DB_PASS, "EMAIL_CONFIG", null, null,
					null);
			if (rs_mail.next()) {
				return new MailConfig().getMailConfig(rs_mail);
			}
			return null;
		} catch (SQLException e) {
			return null;
		} finally {
			try {
				if (rs_mail != null)
					rs_mail.close();
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

	public void insertValues(String db_name, String user_name, String password, String table_name, String column_list,
			String values) throws SQLException {

		sql = "INSERT INTO " + table_name + "(" + column_list + ") VALUES " + values;
		connection = ConnectionDB.createConnection(db_name, user_name, password);
		pst = connection.prepareStatement(sql);
		pst.executeUpdate();
		try {
			if (connection != null)
				connection.close();
			if (pst != null)
				pst.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public ResultSet selectOneField(String db_name, String user_name, String password, String table_name, String field,
			String condition_name, String condition_value, boolean isInteger) {
		ResultSet rs = null;
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

	public void truncateTable(String db_name, String user, String pass, String table_name) throws SQLException {
		try {
			sql = "TRUNCATE " + table_name;
			connection = ConnectionDB.createConnection(db_name, user, pass);
			pst = connection.prepareStatement(sql);
			pst.executeUpdate();
		} catch (SQLException e1) {
			e1.printStackTrace();
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

	public void updateLogs(int id_logs, String name_field, String value, boolean isInteger) {

		sql = "UPDATE LOGS SET " + name_field + "=?,FILE_TIMESTAMP=NOW() WHERE ID=?";
		try {
			connection = ConnectionDB.createConnection(CONTROL_DB_NAME, CONTROL_DB_USER, CONTROL_DB_PASS);
			pst = connection.prepareStatement(sql);
			if (isInteger) {
				pst.setInt(1, Integer.parseInt(value));
			} else {
				pst.setString(1, value);
			}
			pst.setInt(2, id_logs);
			pst.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
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

	public boolean updateProcess(int config_id, String value) {
		String time = value.equals(Process.STATUS_EXTRACT) ? "TIME_DOWNLOAD" : "TIME_EXTRACT";
		sql = "UPDATE PROCESS SET PROCESS_STATUS=?," + time + "=NOW() WHERE CONFIG_ID=?";
		try {
			connection = ConnectionDB.createConnection(CONTROL_DB_NAME, CONTROL_DB_USER, CONTROL_DB_PASS);
			pst = connection.prepareStatement(sql);
			pst.setString(1, value);
			pst.setInt(2, config_id);
			return pst.executeUpdate() > 0;
		} catch (SQLException e) {
			e.printStackTrace();
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

	public String getProcessStatus(int config_id) {
		ResultSet rs_status = null;
		try {
			connection = ConnectionDB.createConnection(CONTROL_DB_NAME, CONTROL_DB_USER, CONTROL_DB_PASS);
			sql = "SELECT PROCESS_STATUS FROM PROCESS WHERE CONFIG_ID=?";
			pst = connection.prepareStatement(sql);
			pst.setInt(1, config_id);
			rs_status = pst.executeQuery();
			if (rs_status.next()) {
				return rs_status.getString(1);
			}
			return null;
		} catch (Exception e) {
			return null;
		} finally {
			try {
				if (rs_status != null)
					rs_status.close();
				if (pst != null)
					pst.close();
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
