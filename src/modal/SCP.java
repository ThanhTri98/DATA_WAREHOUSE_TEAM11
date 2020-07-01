package modal;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SCP {
	private String hostName;
	private int port;
	private String userName;
	private String password;
	private String syncMustMatch;
	private String remotePath;
	private String localPath;

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getSyncMustMatch() {
		return syncMustMatch;
	}

	public void setSyncMustMatch(String syncMustMatch) {
		this.syncMustMatch = syncMustMatch;
	}

	public String getRemotePath() {
		return remotePath;
	}

	public void setRemotePath(String remotePath) {
		this.remotePath = remotePath;
	}

	public String getLocalPath() {
		return localPath;
	}

	public void setLocalPath(String localPath) {
		this.localPath = localPath;
	}

	public SCP getSCP(ResultSet rs) {
		SCP scp = null;
		try {
			rs.next();
			scp = new SCP();
			scp.setHostName(rs.getString("host_name"));
			scp.setPort(rs.getInt("port"));
			scp.setUserName(rs.getString("user_name"));
			scp.setPassword(rs.getString("password"));
			scp.setSyncMustMatch(rs.getString("sync_must_match"));
			scp.setRemotePath(rs.getString("remote_path"));
			scp.setLocalPath(rs.getString("local_path"));
			return scp;
		} catch (SQLException e) {
			return null;
		}
	}

	@Override
	public String toString() {
		return "SCP [hostName=" + hostName + ", port=" + port + ", userName=" + userName + ", password=" + password
				+ ", syncMustMatch=" + syncMustMatch + ", remotePath=" + remotePath + ", localPath=" + localPath + "]";
	}
	

}
