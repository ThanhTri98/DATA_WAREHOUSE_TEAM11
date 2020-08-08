package modal;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;

import dao.ControlDB;
//import util.FileDownLib;

public class DataWarehouse {
	// CONTROL_DB
	private String CONTROL_DB_NAME;
	private String CONTROL_DB_USER;
	private String CONTROL_DB_PASS;
	// CONFIG
	private int CONFIG_ID;
	public static String W_DB_NAME, W_TABLE, W_USER, W_PASS;
	private String IMPORT_DIR, SU_DIR, ERR_DIR, COLUMN_LIST, PROCESS_FUNCTION, STAGING_DB_NAME, STAGING_USER,
			STAGING_PASS, STAGING_TABLE;
	// LOG
	final static String COLUMN_LIST_LOG = "FILE_NAME,CONFIG_ID,FILE_STATUS,STAGING_LOAD_COUNT,WAREHOUSE_LOAD_COUNT,FILE_TIMESTAMP";
	//
	DataProcess d_process;
	ControlDB control_db;

	public DataWarehouse(int id_config) {
		control_db = new ControlDB();
		CONTROL_DB_NAME = control_db.getCONTROL_DB_NAME();
		CONTROL_DB_USER = control_db.getCONTROL_DB_USER();
		CONTROL_DB_PASS = control_db.getCONTROL_DB_PASS();
		// config
		Configuration CONF = control_db.getConfig(id_config);
		CONFIG_ID = CONF.getIdConfig();
		IMPORT_DIR = CONF.getImportDir();
		SU_DIR = CONF.getSuccessDir();
		ERR_DIR = CONF.getErrorDir();
		COLUMN_LIST = CONF.getColmnList();
		PROCESS_FUNCTION = CONF.getProcessFunction();
		W_DB_NAME = CONF.getWarehouseDBName();
		W_USER = CONF.getWarehouseUser();
		W_PASS = CONF.getWarehousePass();
		W_TABLE = CONF.getWarehouseTable();
		STAGING_DB_NAME = CONF.getStagingDBName();
		STAGING_USER = CONF.getStagingUser();
		STAGING_PASS = CONF.getStagingPass();
		STAGING_TABLE = CONF.getStagingTable();
		d_process = new DataProcess();
		SendMail.writeLogsToLocalFile("#" + SendMail.CURRENT_DATE + " " + SendMail.CURRENT_TIME);
	}

	public static void main(String[] args) {
		try {
			DataWarehouse d_warehouse = new DataWarehouse(4);
			d_warehouse.downloadFile();
			d_warehouse.ExtractToDB();
			if (d_warehouse.CONFIG_ID == 4) {
				SendMail.sendMail();
				SendMail.writeLogsToLocalFile("SEND MAIL SUCCESS");
			}
		} catch (NumberFormatException e) {
			System.out.println("Vui long nhap id_config");
		}
	}

//I. funcDownload, funcInsertLog
	public void downloadFile() {
		System.out.println("Wating....");
		// 1.0 Kết nối đến DB controldb -> vào table scp get các thông tin cho việc
		// download file từ server về local
		ResultSet rs_scp = control_db.selectAllField(control_db.getCONTROL_DB_NAME(), control_db.getCONTROL_DB_USER(),
				control_db.getCONTROL_DB_PASS(), "SCP", "CONFIG_ID", CONFIG_ID + "", "true");
//		 1.1Lưu vào đối tượng SCP
		SCP scp = new SCP().getSCP(rs_scp);
		if (scp == null) { // Cho trường hợp không download file Subject
			System.out.println("Null download");
			return;
		}
		// 1.2 Kết nối đến DB controldb -> vào table logs lấy ra danh sách các file_name
		// (không down lại file)
//		String not_match = "";
//		ResultSet rs_file_name = control_db.selectOneField(CONTROL_DB_NAME, CONTROL_DB_USER, CONTROL_DB_PASS, "LOGS",
//				"FILE_NAME", null, null, false);
//		try {
//			while (rs_file_name.next()) {
//				not_match += rs_file_name.getString(1) + ";";
//			}
//		} catch (SQLException e1) {
//			e1.printStackTrace();
//		}
		// 1.3 Tiến hành download file (isSU=0 ->success, =-1 error)
//		int isSU = FileDownLib.fileDownload(scp.getHostName(), scp.getPort(), scp.getUserName(), scp.getPassword(),
//				scp.getRemotePath(), scp.getLocalPath(), scp.getSyncMustMatch(), not_match);
		int isSU = 0;
		if (isSU == 0) {
			// 1.4 Download thành công -> tiến hành ghi log với file_status là ER
			insertLog(scp.getLocalPath(), "ER");
			SendMail.writeLogsToLocalFile("DOWNLOAD FILE - " + SendMail.CURRENT_TIME);
		} else {
			SendMail.writeLogsToLocalFile("!!!DOWNLOAD FAIL");
			System.out.println("Download Fail!!!"); // send mail
		}

		try {
			rs_scp.close();
//			rs_file_name.close();
		} catch (SQLException e) {
			SendMail.writeLogsToLocalFile("  !!!" + e.getMessage());
			e.printStackTrace();
		}
	}

	public void insertLog(String folder, String file_status) {
		// 1.6 Mở folder và kiểm tra folder tồn tại hay không?
		System.out.println("File downloaded");
		File fd = new File(folder);
		if (!fd.exists())
			return;
		StringBuilder value = new StringBuilder();
		// 1.7 Nếu tồn tại thư mục thì lưu tất cả các file ở trong thư mục đó vào mảng
		// File[]
		File[] listFile = fd.listFiles();
		for (File file : listFile) {
			SendMail.writeLogsToLocalFile(" -> " + file.getName());
			System.out.println(file.getName());
			// Các file có định dạng ở dưới là hợp lệ
			if (file.getPath().endsWith(".xlsx") || file.getPath().endsWith(".txt")
					|| file.getPath().endsWith(".csv")) {
				// 1.8 Tạo value cho câu sql INSERT INTO TABLE VALUES value
				value.append("('" + file.getName() + "'");
				value.append("," + CONFIG_ID);
				value.append(",'" + file_status + "'");
				value.append("," + -1);// staging_load_count (giá trị mặc định -1)
				value.append("," + -1);// warehouse_load_count (giá trị mặc định -1)
				value.append(",NOW())");
				// 1.9 Mở kết nối DB controldb -> Insert thông tin của từng file xuống table
				// logs
				try {
					control_db.insertValues(CONTROL_DB_NAME, CONTROL_DB_USER, CONTROL_DB_PASS, "LOGS", COLUMN_LIST_LOG,
							value.toString());
					value = new StringBuilder();
					// 1.10 Move file qua thư mục import_dir phục vụ cho bước 2.
					d_process.moveFile(file, IMPORT_DIR);
				} catch (SQLException e) {
					SendMail.writeLogsToLocalFile("  !!!" + e.getMessage());
					e.printStackTrace();
				}
			}
		}
		SendMail.writeLogsToLocalFile("INSERT LOG: ER - " + SendMail.CURRENT_TIME);
	}

	// II,III ExtractToStaging --> Transform --> Warehouse
	public void ExtractToDB() {
		SendMail.writeLogsToLocalFile("EXTRACT TO DATABASE - " + SendMail.CURRENT_TIME);
		System.out.println("Extract Staging...");
		// 2.0 Lấy ra tất cả các trường có file_status=ER và lưu vào ResultSet
		ResultSet allRecordLogs = control_db.selectAllField(CONTROL_DB_NAME, CONTROL_DB_USER, CONTROL_DB_PASS, "LOGS",
				"FILE_STATUS,CONFIG_ID", "ER," + CONFIG_ID, "false,true");
		try {
			File file = null;
			String file_name = null;
			int id_log = -999; // default
			int countLines = -999;
			while (allRecordLogs.next()) {
				// 2.1 Lấy ra tên file và id trong bảng ghi
				file_name = allRecordLogs.getString("FILE_NAME");
				id_log = allRecordLogs.getInt("ID");
				String values = "";// Lưu chuỗi values
				// 2.2 Mở đối tượng file nằm trong thư mục IMPORT_DIR dựa vào file_name
				file = new File(IMPORT_DIR + File.separator + file_name);
				if (!file.exists()) {
					SendMail.writeLogsToLocalFile(" -> " + file.getAbsolutePath() + " STATUS: " + "FILE NOT EXISTS");
					continue;
				}
				// 2.3 Tiến hành đọc file và chuyển nội dung trong file thành
				// chuỗi values (1,'a','b'),(2,'d','e'),(...)
				// INSERT INTO TABLE VALUES chuỗi values
				if (file.getPath().endsWith(".xlsx")) {
					values = d_process.readValuesXLSX(file, COLUMN_LIST);
				} else if (file.getPath().endsWith(".txt") || file.getPath().endsWith(".csv")) {
					values = d_process.readValuesTXT(file, COLUMN_LIST);
				}
				try {
					// Tách lấy số dòng đọc lên từ file
					if (values != null && !values.isEmpty()) {
						int index = values.indexOf(DataProcess.DELIM_COUNT_LINES);
						countLines = Integer.parseInt(values.substring(0, index));
						values = values.replace(countLines + DataProcess.DELIM_COUNT_LINES, "");
					}
					// Truncate table staging trước khi đưa dữ liệu mới vào
					control_db.truncateTable(STAGING_DB_NAME, STAGING_USER, STAGING_PASS, STAGING_TABLE);
					// 2.4 Tiến hành insert chuỗi values xuống table student trong db_staging đồng
					// thời transform rồi đưa qua warehouse
					control_db.insertValues(STAGING_DB_NAME, STAGING_USER, STAGING_PASS, STAGING_TABLE, COLUMN_LIST,
							values);
					// Cập nhật số dòng vừa load vào db_staging
					control_db.updateLogs(CONTROL_DB_NAME, CONTROL_DB_USER, CONTROL_DB_PASS, id_log,
							"STAGING_LOAD_COUNT", countLines + "", true);
					System.out.println(file_name + "--> Transforming...");
					// Lấy toàn bộ bảng ghi trong table student từ db staging
					ResultSet data_staging = control_db.selectAllField(STAGING_DB_NAME, STAGING_USER, STAGING_PASS,
							STAGING_TABLE, null, null, null);
					// 2.5 Tiến hành transform dữ liệu và chuyển qua table student trong db
					// warehouse và trả về số dòng vừa chuyển qua dw
					int warehouse_load_count = d_process.transferData(data_staging, id_log, COLUMN_LIST,
							PROCESS_FUNCTION);
					// Update log when insert data success
					// Nếu số dòng lớn hơn 0 có nghĩa là không có trường nào bị lỗi format ( trans
					// thành công ít nhất 1 dòng)
					if (warehouse_load_count >= 0) {
						// Cập nhật file_status = SU
						String status = warehouse_load_count == countLines ? "SU"
								: warehouse_load_count == 0 ? "FILE_DUPLICATE" : "MISSING_TRAN";
						control_db.updateLogs(CONTROL_DB_NAME, CONTROL_DB_USER, CONTROL_DB_PASS, id_log, "FILE_STATUS",
								status, false);
						// Cập nhật số dòng load vào table student trong dw
						control_db.updateLogs(CONTROL_DB_NAME, CONTROL_DB_USER, CONTROL_DB_PASS, id_log,
								"WAREHOUSE_LOAD_COUNT", warehouse_load_count + "", true);
						// xong thì tiến hành chuyển file chứa dữ liệu vừa rồi qua thư mục SUCCESS_DIR
						d_process.moveFile(file, SU_DIR);
						if (status.equals("MISSING_TRAN")) {
							SendMail.writeLogsToLocalFile(" -> " + file_name + " STATUS: " + status + " "
									+ (countLines - warehouse_load_count) + " LINES");
							System.out.println(file_name + " TRAN STATUS: ->  " + status + " "
									+ (countLines - warehouse_load_count) + " LINES");
						} else if (status.equals("SU")) {
							SendMail.writeLogsToLocalFile(
									" -> " + file_name + " STATUS: " + status + " " + warehouse_load_count + " LINES");
							System.out.println(
									file_name + " TRAN STATUS: ->  " + status + " " + warehouse_load_count + " LINES");
						} else {
							SendMail.writeLogsToLocalFile(" -> " + file_name + " STATUS: " + status);
							System.out.println(file_name + " TRAN STATUS: ->  " + status);
						}
						SendMail.writeLogsToLocalFile("#	#	#	#	#	#	#	#");
					} else {
						String status = warehouse_load_count == -2 ? "ERR_TRAN"
								: warehouse_load_count == -1 ? "FK_NOT_EXISTS" : "DUPLI&FK_NOT_EXISTS";
						control_db.updateLogs(CONTROL_DB_NAME, CONTROL_DB_USER, CONTROL_DB_PASS, id_log, "FILE_STATUS",
								status, false);
						// Đồng thời chuyển file vừa rồi vào thục mục ERR_DIR
						d_process.moveFile(file, ERR_DIR);
						SendMail.writeLogsToLocalFile(" -> " + file_name + " STATUS -> " + status);
						SendMail.writeLogsToLocalFile("#	#	#	#	#	#	#	#");
						System.out.println(" -> " + file_name + " STATUS -> " + status);
					}
				} catch (SQLException | NumberFormatException | StringIndexOutOfBoundsException e) {
					// File không đúng format thì TỰ DÔ MÀ SỬA :))
					control_db.updateLogs(CONTROL_DB_NAME, CONTROL_DB_USER, CONTROL_DB_PASS, id_log, "FILE_STATUS",
							"ERR_STAGING", false);
					SendMail.writeLogsToLocalFile(" -> " + file_name + " STATUS: ERR_STAGING");
					SendMail.writeLogsToLocalFile(" -> " + file_name + " !!!ERR " + e.getMessage());
					SendMail.writeLogsToLocalFile("#	#	#	#	#	#	#	#");
					System.out.println(file_name + " -> ERR_STAGING");
					// Đưa qua thư mục lỗi thâu
					d_process.moveFile(file, ERR_DIR);
					continue;
				}
			}
			System.out.println("Complete");
		} catch (SQLException e) {
			e.printStackTrace();
			SendMail.writeLogsToLocalFile("  !!!" + e.getMessage());
		} finally {
			try {
				allRecordLogs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}