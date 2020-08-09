package modal;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import dao.ControlDB;

public class Process {
	public static final String STATUS_DOWNLOAD = "DOWNLOAD";
	public static final String STATUS_EXTRACT = "EXTRACT";
	private int congif_id;
	private String process_status;

	public int getCongif_id() {
		return congif_id;
	}

	public void setCongif_id(int congif_id) {
		this.congif_id = congif_id;
	}

	public String getProcess_status() {
		return process_status;
	}

	public void setProcess_status(String process_status) {
		this.process_status = process_status;
	}

	public Process getProcess(ResultSet rs) {
		try {
			this.setCongif_id(rs.getInt("config_id"));
			this.setProcess_status(rs.getString("process_status"));
			return this;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public void run() {
		List<Process> listProcess = new ControlDB().getListProcess();
		for (int i = 0; i < listProcess.size(); i++) {
			DataWarehouse dataWarehouse = new DataWarehouse(listProcess.get(i).getCongif_id());
			dataWarehouse.downloadFile();
			dataWarehouse.ExtractToDB();
			if (i == listProcess.size() - 1) {
				SendMail.sendMail();
			}
		}
	}

	public static void main(String[] args) {
		new Process().run();
	}
}
