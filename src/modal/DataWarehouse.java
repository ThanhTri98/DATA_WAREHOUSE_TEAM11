package modal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import dao.ControlDB;
import util.FileDownLib;

public class DataWarehouse {
	static final String EXT_TEXT = ".txt";
	static final String EXT_CSV = ".csv";
	static final String EXT_EXCEL = ".xlsx";
	// config
	static final Configuration conf = ControlDB.getConfig();
	static final int CONFIG_ID = conf.getIdConfig();
	static final String IMPORT_DIR = conf.getImportDir();
	static final String SU_DIR = conf.getSuccessDir();
	static final String ERR_DIR = conf.getErrorDir();
	static final String COLUMN_LIST = conf.getColmnList();
	static final String DELIM = conf.getDelimiter();
	static final String W_DB_NAME = conf.getWarehouseDBName();
	static final String W_USER = conf.getWarehouseUser();
	static final String W_PASS = conf.getWarehousePass();
	static final String W_TABLE = conf.getWarehouseTable();
	static final String STAGING_DB_NAME = conf.getStagingDBName();
	static final String STAGING_USER = conf.getStagingUser();
	static final String STAGING_PASS = conf.getStagingPass();
	static final String STAGING_TABLE = conf.getStagingTable();
	// logs
	static final String COLUMN_LIST_LOGS = "file_name,config_id,file_status,staging_load_count,warehouse_load_count,file_timestamp";
	//
	DataProcess d_process;

	public DataWarehouse() {
		d_process = new DataProcess();
		SendMail.writeLogsToLocalFile("#"+SendMail.CURRENT_DATE+" "+SendMail.CURRENT_TIME);
	}

	public static void main(String[] args) {
		DataWarehouse d_warehouse = new DataWarehouse();
		d_warehouse.downloadFile();
		d_warehouse.ExtractToDB();
		d_warehouse.loadSubjects();
		SendMail.sendMail();
	}

	/**
	 * I. Tải file về thư mục C:\WAREHOUSE\SCP dùng SCP ### 1. Tải hoàn tất thì quét
	 * thư mục SCP và ghi log -> file_status = ER -> Move file vừa ghi log vào
	 * C:\WAREHOUSE\IMPORT_DIR, Kiểm tra file đó đã được import vào hệ thống
	 * chưa(file_status=TR,SU), nếu tồn tại thì không tải nữa ### 2. Vào bảng Logs
	 * (trong DB control_db) đọc tất cả records, nếu rcd đó có file_status = ER thì
	 * ghi toàn bộ nội dung của file đó vào table student (trong DB db_staging) ->
	 * đồng thời chuyển trạng thái file đó thành TR
	 */
	/**
	 * II. Tiến hành tranform dữ liệu ### 1. Vào bảng Logs (trong DB control_db) đọc
	 * tất cả records, nếu rcd đó có file_status = TR -> vào bảng student (trong DB
	 * db_staging) đọc tất cả các rcd có trường file_name = file hiện tại có
	 * file_status = TR xong thì tiến hàng transform dữ liệu ### 2. Sau khi đã
	 * tranform tất cả các dòng trong file thì lưu lại số dòng đã trans, nếu số dòng
	 * đã trans = số dòng đọc lên từ file thì chuyển trạng thái file đó thành SU,
	 * ngược lại thì ERR -> Move các file vào C:\WAREHOUSE\ERROR_DIR???
	 */
	/**
	 * III. Tiến hành ghi các file có file_status = SU vào bảng student trong DB
	 * warehouse ### 1. Vào bảng Logs (trong DB control_db) đọc tất cả các records,
	 * nếu rcd nào có file_status = SU thì tiến hành move dữ liệu từ bảng student
	 * (trong DB db_staging) qua bảng student (trong DB warehouse) ### 2. Quá trình
	 * di chuyển hoàn tất -> Move các file đó vào thư mục C:\WAREHOUSE\SUCCESS_DIR
	 * -> Kết thúc chu trình.
	 * 
	 * -------Nếu file_status = ERR thì làm lại B1 nhưng thay thư mục SCP thành
	 * C:\WAREHOUSE\ERROR_DIR
	 */

//I. funcDownload, funcInsertLog
	public void downloadFile() {
		System.out.println("Wating....");
		// 1.0 Kết nối đến DB controldb -> vào table scp get các thông tin cho việc
		// download file từ server về local
		ResultSet rs_scp = ControlDB.selectAllField(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
				ControlDB.CONTROL_DB_PASS, "scp", null, null, false);
		// 1.1Lưu vào đối tượng SCP
		SCP scp = new SCP().getSCP(rs_scp);
		// 1.2 Kết nối đến DB controldb -> vào table logs lấy ra danh sách các file_name
		// (không down lại file)
		String not_match = "";
		ResultSet rs_file_name = ControlDB.selectOneField(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
				ControlDB.CONTROL_DB_PASS, "logs", "file_name", null, null);
		try {
			while (rs_file_name.next()) {
				not_match += rs_file_name.getString(1) + ";";
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		// 1.3 Tiến hành download file (isSU=0 ->success, =-1 error)
		int isSU = FileDownLib.fileDownload(scp.getHostName(), scp.getPort(), scp.getUserName(), scp.getPassword(),
				scp.getRemotePath(), scp.getLocalPath(), scp.getSyncMustMatch(), not_match);

		if (isSU == 0) {
			// 1.4 Download thành công -> tiến hành ghi log với file_status là ER
			SendMail.writeLogsToLocalFile("DOWNLOAD FILE - "+SendMail.CURRENT_TIME);
			insertLog(scp.getLocalPath(), "ER");
		} else {
			SendMail.writeLogsToLocalFile("!!!DOWNLOAD FAIL");
			System.out.println("Download Fail!!!"); // send mail
		}

		try {
			rs_scp.close();
			rs_file_name.close();
		} catch (SQLException e) {
			SendMail.writeLogsToLocalFile("  !!!"+e.getMessage());
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
			SendMail.writeLogsToLocalFile(" -> "+file.getName());
			System.out.println(file.getName());
			// Các file có định dạng ở dưới là hợp lệ
			if (file.getPath().endsWith(EXT_EXCEL) || file.getPath().endsWith(EXT_TEXT)
					|| file.getPath().endsWith(EXT_CSV)) {
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
					ControlDB.insertValues(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
							ControlDB.CONTROL_DB_PASS, "Logs", COLUMN_LIST_LOGS, value.toString());
					value = new StringBuilder();
					// 1.10 Move file qua thư mục import_dir phục vụ cho bước 2.
					d_process.moveFile(file, IMPORT_DIR);
				} catch (SQLException e) {
					SendMail.writeLogsToLocalFile("  !!!"+e.getMessage());
					e.printStackTrace();
				}

			}
		}
		SendMail.writeLogsToLocalFile("INSERT LOG: ER - "+SendMail.CURRENT_TIME);
	}

	// II,III ExtractToStaging --> Transform --> Warehouse
	public void ExtractToDB() {
		SendMail.writeLogsToLocalFile("EXTRACT TO DATABASE - "+SendMail.CURRENT_TIME);
		System.out.println("Extract Staging...");
		// 2.0 Lấy ra tất cả các trường có file_status=ER và lưu vào ResultSet
		ResultSet allRecordLogs = ControlDB.selectAllField(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
				ControlDB.CONTROL_DB_PASS, "LOGS", "file_status", "ER", false);
		try {
			File file = null;
			String file_name = null;
			int file_id = -999; // default
			String extention;
			while (allRecordLogs.next()) {
				// 2.1 Lấy ra tên file và id trong bảng ghi
				file_name = allRecordLogs.getString("file_name");
				file_id = allRecordLogs.getInt("id");
				String values = "";// Lưu chuỗi values
				// 2.2 Mở đối tượng file nằm trong thư mục IMPORT_DIR dựa vào file_name
				file = new File(IMPORT_DIR + File.separator + file_name);
				extention = file.getPath().endsWith(".xlsx") ? EXT_EXCEL // Phục vụ cho method đếm số dòng -> Staging
						: file.getPath().endsWith(".txt") ? EXT_TEXT : EXT_CSV;
				if (!file.exists()) // Nếu file không tồn tại thì bỏ qua và tiếp tục cho đến cuối bảng ghi
					continue;
				// Đếm số cột theo format trong table config
				StringTokenizer count_Field = new StringTokenizer(COLUMN_LIST, DELIM);
				// 2.3 Tiến hành đọc file và chuyển nội dung trong file thành
				// chuỗi values (1,'a','b'),(2,'d','e'),(...)
				// INSERT INTO TABLE VALUES chuỗi values
				if (extention.equals(EXT_EXCEL)) {
					values = d_process.readValuesXLSX(file, file_id, count_Field.countTokens());
				} else if (extention.equals(EXT_TEXT)) {
					values = d_process.readValuesTXT(file, file_id, count_Field.countTokens());
				} else if (extention.equals(EXT_CSV)) {
				}
				try {
					// 2.4 Tiến hành insert chuỗi values xuống table student trong db_staging đồng
					// thời transform rồi đưa qua warehouse
					if (ControlDB.insertValues(STAGING_DB_NAME, STAGING_USER, STAGING_PASS, STAGING_TABLE,
							COLUMN_LIST + ",id_log", values)) {
						// Cập nhật số dòng vừa load vào db_staging
						ControlDB.updateLogs(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
								ControlDB.CONTROL_DB_PASS, file_id, "staging_load_count",
								d_process.countLines(file, extention) + "", true);
						System.out.println(file_name + "--> Transforming...");
						// Lấy toàn bộ bảng ghi trong table student từ db staging
						ResultSet data_staging = ControlDB.selectAllField(STAGING_DB_NAME, STAGING_USER, STAGING_PASS,
								STAGING_TABLE, null, null, false);
						// 2.5 Tiến hành transform dữ liệu và chuyển qua table student trong db
						// warehouse và trả về số dòng vừa chuyển qua dw
						int warehouse_load_count = d_process.transformData(data_staging);
						// Update log when insert data success
						// Nếu số dòng lớn hơn 0 có nghĩa là không có trường nào bị lỗi format ( trans
						// thành công ít nhất 1 dòng)
						if (warehouse_load_count > 0) {
							// Cập nhật file_status = SU
							ControlDB.updateLogs(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
									ControlDB.CONTROL_DB_PASS, file_id, "file_status", "SU", false);
							// Cập nhật số dòng load vào table student trong dw
							ControlDB.updateLogs(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
									ControlDB.CONTROL_DB_PASS, file_id, "warehouse_load_count",
									warehouse_load_count + "", true);
							// xong thì tiến hành chuyển file chứa dữ liệu vừa rồi qua thư mục SUCCESS_DIR
							d_process.moveFile(file, SU_DIR);
							SendMail.writeLogsToLocalFile(" -> "+file_name+" STATUS: SU");
							System.out
									.println(file_name + " Transform success -> SU " + warehouse_load_count + " lines");
						} else {
							// Không dòng nào có tất cả các trường đúng định dạng-> tự mở file sửa
							// Cập nhật file_status file dừa rồi là ERR_TRAN
							ControlDB.updateLogs(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
									ControlDB.CONTROL_DB_PASS, file_id, "file_status", "ERR_TRAN", false);
							// Đồng thời chuyển file vừa rồi vào thục mục ERR_DIR
							d_process.moveFile(file, ERR_DIR);
							SendMail.writeLogsToLocalFile(" -> "+file_name+" STATUS: ERR_TRAN");
							System.out.println(file_name + " Transform error -> ERR_TRAN");
						}
					}
				} catch (SQLException e) {
					// File không đúng format thì chịu :))
					ControlDB.updateLogs(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
							ControlDB.CONTROL_DB_PASS, file_id, "file_status", "ERR_STAGING", false);
					SendMail.writeLogsToLocalFile(" -> "+file_name+" STATUS: ERR_STAGING");
					System.out.println(file_name + "--> ERR_STAGING");
					// Đưa qua thư mục lỗi thâu
					d_process.moveFile(file, ERR_DIR);
					continue;
				}
			}
			System.out.println("Complete");
		} catch (SQLException e) {
			e.printStackTrace();
			SendMail.writeLogsToLocalFile("  !!!"+e.getMessage());
		} finally {
			try {
				allRecordLogs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	@SuppressWarnings("static-access")
	public void loadSubjects() {
		SendMail.writeLogsToLocalFile("LOAD SUBJECTS - "+SendMail.CURRENT_TIME);
		System.out.println("Start load subject");
		// Lấy bảng ghi chưa load vào DW dựa vào field Loaded(0 chưa, 1 rồi)
		ResultSet conf_subjects = ControlDB.selectAllField(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
				ControlDB.CONTROL_DB_PASS, "conf_subjects", "loaded", "0", true);
		try {
			while (conf_subjects.next()) {
				String path_file = conf_subjects.getString("path_file");
				SendMail.writeLogsToLocalFile(" -PATH FILE: "+path_file);
				System.out.println("File loadding...");
				System.out.println(path_file);
				String delim = conf_subjects.getString("delim");
				String column_list = conf_subjects.getString("column_list");
				int id_conf_subj = conf_subjects.getInt("id");
				int stt;
				int ma_mh;
				String ten_mh;
				int tin_chi;
				String khoa_bm_ql;
				String khoa_bm_sd;
				String TIME_EXPIRE = "2013-12-31";
				StringTokenizer str;
				String values = "";
				try {
					BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path_file)),"utf8"));
					String line = bReader.readLine(); //***
					// Map chứa tất cả môn học hiện có trong bảng MONHOC với key là stt của môn học đó
					Map<Integer, Subjects> Subjects_Map = ControlDB.loadSubject(W_DB_NAME, W_USER, W_PASS, "MONHOC");
					if(Subjects_Map.size()==0) {
						System.out.println("Please import default data Subjects");
						bReader.close();
						return;
					}
					// Kiểm tra để loại bỏ phần tiêu đề
					// Nếu không có phần tiêu đề thì đặt lại con trỏ về trị trí ban đầu vì sau *** con trỏ tăng lên 1
					if (Pattern.matches(d_process.NUMBER_REGEX, line.split(delim)[0])) {
						bReader = new BufferedReader(new FileReader(new File(path_file)));
					}
					while ((line = bReader.readLine()) != null) {
						// Nếu mà có thay đổi thì insert dòng mới và update time_expire dòng cũ thành **
						// 2013-12-31;
						str = new StringTokenizer(line, delim);
						stt = Integer.parseInt(str.nextToken());
						Subjects subj = Subjects_Map.get(stt);
						// Nếu nội dung của 2 dòng là khác nhau thì thêm dòng mới với time_expire =
						// default và cập nhật lại time_expire cũ thành 2013-12-31
						if (!line.contains(subj.toString().trim())) {
							ma_mh = Integer.parseInt(str.nextToken());
							ten_mh = str.nextToken();
							tin_chi = Integer.parseInt(str.nextToken());
							khoa_bm_ql = str.nextToken();
							khoa_bm_sd = "";
							// VD: (1,2042212,'Data Warehouse',3,'CNTT','CNTT')
							values = "(" + stt + "," + ma_mh + ",'" + ten_mh + "'," + tin_chi + ",'" + khoa_bm_ql + "',"
									+ "'" + khoa_bm_sd + "'" + ")";
							// Insert values vào bảng MONHOC trong DB warehouse
							ControlDB.insertValues(W_DB_NAME, W_USER, W_PASS, "MONHOC", column_list, values);
							// Cập nhật time_expire lại cho dòng trước đó dựa vào id
							ControlDB.updateOneFieldByID(W_DB_NAME, W_USER, W_PASS, "MONHOC", "TIME_EXPIRE",
									TIME_EXPIRE, subj.getId(), false);
							values = "";
						}else {
							SendMail.writeLogsToLocalFile("  -> DUPLICATE DATA: STT = "+stt);
							System.out.println("Duplicate data: STT = "+stt);
						}
					}
					// Cập nhật lại trạng thái loaded = 1 (đã load)
					ControlDB.updateOneFieldByID(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
							ControlDB.CONTROL_DB_PASS, "CONF_SUBJECTS", "LOADED", 1 + "", id_conf_subj, true);
					System.out.println("Load Subject Complete");
					bReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		} catch (SQLException e) {
			SendMail.writeLogsToLocalFile("  !!!"+e.getMessage());
			e.printStackTrace();
		}
	}
}