package modal;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Logs {
	private String fileName;
	private int configId;
	private String fileStatus;
	private int stagingLoadCount;
	private String fileTimestamp;

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public int getConfigId() {
		return configId;
	}

	public void setConfigId(int configId) {
		this.configId = configId;
	}

	public String getFileStatus() {
		return fileStatus;
	}

	public void setFileStatus(String fileStatus) {
		this.fileStatus = fileStatus;
	}

	public int getStagingLoadCount() {
		return stagingLoadCount;
	}

	public void setStagingLoadCount(int stagingLoadCount) {
		this.stagingLoadCount = stagingLoadCount;
	}

	public String getFileTimestamp() {
		return fileTimestamp;
	}

	public void setFileTimestamp(String fileTimestamp) {
		this.fileTimestamp = fileTimestamp;
	}

	public Logs getLogs(ResultSet rs) {
		Logs log = null;
		try {
			log = new Logs();
			log.setFileName(rs.getString("file_name"));
			log.setConfigId(rs.getInt("config_id"));
			log.setFileStatus(rs.getString("file_status"));
			log.setStagingLoadCount(rs.getInt("staging_load_count"));
			log.setFileTimestamp(rs.getString("file_timestamp"));
			return log;
		} catch (SQLException e) {
			return null;
		}
	}

}
