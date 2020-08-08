package modal;

import java.sql.ResultSet;
import java.sql.SQLException;

public class MailConfig {
	private String hostName;
	private int sslPort;
	private String appEmail;
	private String appPassword;
	private String receiveEmail;
	private String localPath;
	
	public String getLocalPath() {
		return localPath;
	}
	public void setLocalPath(String localPath) {
		this.localPath = localPath;
	}
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public int getSslPort() {
		return sslPort;
	}
	public void setSslPort(int sslPort) {
		this.sslPort = sslPort;
	}
	public String getAppEmail() {
		return appEmail;
	}
	public void setAppEmail(String appEmail) {
		this.appEmail = appEmail;
	}
	public String getAppPassword() {
		return appPassword;
	}
	public void setAppPassword(String appPassword) {
		this.appPassword = appPassword;
	}
	public String getReceiveEmail() {
		return receiveEmail;
	}
	public void setReceiveEmail(String receiveEmail) {
		this.receiveEmail = receiveEmail;
	}
	public MailConfig getMailConfig(ResultSet rs) {
		try {
			this.setHostName(rs.getString("host_name"));
			this.setSslPort(rs.getInt("ssl_port"));
			this.setAppEmail(rs.getString("app_email"));
			this.setAppPassword(rs.getString("app_password"));
			this.setReceiveEmail(rs.getString("receive_email"));
			this.setLocalPath(rs.getString("local_path"));
			return this;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

}
