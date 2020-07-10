package modal;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

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
	static final String COLUMN_LIST_LOGS = "file_name,config_id,file_status,staging_load_count,file_timestamp";
	//
	DataProcess d_process;

	public DataWarehouse() {
		d_process = new DataProcess();
	}

	public static void main(String[] args) {
		DataWarehouse d_warehouse = new DataWarehouse();
//		d_warehouse.downloadFile();

	}

	/*
	 * I. Tải file về thư mục C:\WAREHOUSE\SCP dùng SCP ### 1. Tải hoàn tất thì quét
	 * thư mục SCP và ghi log -> file_status = ER -> Move file vừa ghi log vào
	 * C:\WAREHOUSE\IMPORT_DIR, Kiểm tra file đó đã được import vào hệ thống
	 * chưa(file_status=TR,SU), nếu tồn tại thì không tải nữa ### 2. Vào bảng Logs
	 * (trong DB control_db) đọc tất cả records, nếu rcd đó có file_status = ER thì
	 * ghi toàn bộ nội dung của file đó vào table student (trong DB db_staging) ->
	 * đồng thời chuyển trạng thái file đó thành TR
	 */
	/*
	 * II. Tiến hành tranform dữ liệu ### 1. Vào bảng Logs (trong DB control_db) đọc
	 * tất cả records, nếu rcd đó có file_status = TR -> vào bảng student (trong DB
	 * db_staging) đọc tất cả các rcd có trường file_name = file hiện tại có
	 * file_status = TR xong thì tiến hàng transform dữ liệu ### 2. Sau khi đã
	 * tranform tất cả các dòng trong file thì lưu lại số dòng đã trans, nếu số dòng
	 * đã trans = số dòng đọc lên từ file thì chuyển trạng thái file đó thành SU,
	 * ngược lại thì ERR -> Move các file vào C:\WAREHOUSE\ERROR_DIR???
	 */
	/*
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
				ControlDB.CONTROL_DB_PASS, "scp", null, null);
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
//			System.out.println("DownFile success."); // send mail
			insertLog(scp.getLocalPath(), "ER");
		} else {
			System.out.println("Download Fail!!!"); // send mail
		}

		try {
			rs_scp.close();
			rs_file_name.close();
		} catch (SQLException e) {
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
			System.out.println(file.getName());
			// Các file có định dạng ở dưới là hợp lệ
			if (file.getPath().endsWith(EXT_EXCEL) || file.getPath().endsWith(EXT_TEXT)
					|| file.getPath().endsWith(EXT_CSV)) {
				// 1.8 Tạo value cho câu sql INSERT INTO TABLE VALUES value
				value.append("('" + file.getName() + "'");
				value.append("," + CONFIG_ID);
				value.append(",'" + file_status + "'");
				value.append("," + 0);
				value.append(",NOW())");
				try {
					// 1.9 Mở kết nối DB controldb -> Insert thông tin của từng file xuống table
					// logs
					ControlDB.insertValues(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
							ControlDB.CONTROL_DB_PASS, "Logs", COLUMN_LIST_LOGS, value.toString());
					value = new StringBuilder();
					// 1.10 Move file qua thư mục import_dir phục vụ cho bước 2.
					moveFile(file, IMPORT_DIR);
				} catch (SQLException e) {
					e.printStackTrace();
				}

			}
		}
	}

	// II ExtractToStaging
		public void ExtractToStaging() {
			System.out.println("Extract Staging...");
			// 2.0 Lấy ra tất cả các trường có file_status=ER và lưu vào ResultSet
			ResultSet allRecordLogs = ControlDB.selectAllField(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
					ControlDB.CONTROL_DB_PASS, "LOGS", "file_status", "ER");
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
						// 2.4 Tiến hành insert chuỗi values xuống table student trong db_staging
						if (ControlDB.insertValues(STAGING_DB_NAME, STAGING_USER, STAGING_PASS, STAGING_TABLE,
								COLUMN_LIST + ",id_log", values)) {
							ControlDB.updateCountLines(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
									ControlDB.CONTROL_DB_PASS, file_id, "staging_load_count", countLines(file, extention));
							System.out.println(file_name + "--> Transforming...");
							// 2.5 Tiến hành transform dữ liệu từ staging
							ResultSet data_staging = ControlDB.selectAllField(STAGING_DB_NAME, STAGING_USER, STAGING_PASS,
									STAGING_TABLE, null, null);
							int warehouse_load_count = d_process.transformData(data_staging);
							ControlDB.updateCountLines(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
									ControlDB.CONTROL_DB_PASS, file_id,"warehouse_load_count", warehouse_load_count);
//								ControlDB.updateFileStatus(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
//										ControlDB.CONTROL_DB_PASS, file_id, "TR");

						}
					} catch (SQLException e) {
						ControlDB.updateFileStatus(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
								ControlDB.CONTROL_DB_PASS, file_id, "ERR_STAGING");
						System.out.println(file_name + "--> ERR");
						moveFile(file, ERR_DIR);
						continue;
					}
				}
				System.out.println("Complete");
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					allRecordLogs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

	private boolean moveFile(File file, String target_dir) {
		try {
			BufferedInputStream bReader = new BufferedInputStream(new FileInputStream(file));
			BufferedOutputStream bWriter = new BufferedOutputStream(
					new FileOutputStream(target_dir + File.separator + file.getName()));
			byte[] buff = new byte[1024 * 10];
			int data = 0;
			while ((data = bReader.read(buff)) != -1) {
				bWriter.write(buff, 0, data);
			}
			bReader.close();
			bWriter.close();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} finally {
			file.delete();
		}
	}

	private int countLines(File file, String extention) {
		int result = 0;
		XSSFWorkbook workBooks = null;
		try {
			if (extention.indexOf(EXT_TEXT) != -1) {
				BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
				String line;
				while ((line = bReader.readLine()) != null) {
					if (!line.trim().isEmpty()) {
						result++;
					}
				}
				bReader.close();
			} else if (extention.indexOf(EXT_EXCEL) != -1) {
				workBooks = new XSSFWorkbook(file);
				XSSFSheet sheet = workBooks.getSheetAt(0);
				Iterator<Row> rows = sheet.iterator();
				rows.next();
				while (rows.hasNext()) {
					rows.next();
					result++;
				}
			}
			return result;
		} catch (IOException | org.apache.poi.openxml4j.exceptions.InvalidFormatException e) {
			e.printStackTrace();
			return 0;
		} finally {
			if (workBooks != null) {
				try {
					workBooks.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}