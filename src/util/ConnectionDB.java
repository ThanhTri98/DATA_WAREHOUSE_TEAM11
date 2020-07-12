package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import modal.SendMail;

public class ConnectionDB {
	public static Connection createConnection(String db_name, String user_name, String password) {
		Connection con = null;
		String url = db_name;
		try {
			if (con == null || con.isClosed()) {
				Class.forName("com.mysql.jdbc.Driver");
				con = DriverManager.getConnection(url, user_name, password);
			}
			return con;

		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
			SendMail.writeLogsToLocalFile("  !!!ERROR CONNECTED TO DATABASE");
			return null;
		}
	}
}
