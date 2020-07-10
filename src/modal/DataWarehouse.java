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
	static final String ER_DIR = conf.getErrorDir();
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
		d_warehouse.checkFileStatus();

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
		ResultSet rs = ControlDB.selectAllField(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
				ControlDB.CONTROL_DB_PASS, "scp");
		SCP scp = new SCP().getSCP(rs);
		int success = FileDownLib.fileDownload(scp.getHostName(), scp.getPort(), scp.getUserName(), scp.getPassword(),
				scp.getRemotePath(), scp.getLocalPath(), scp.getSyncMustMatch());
		if (success == -1) {
			// send mail bao loi khong ket noi duoc toi server
			System.out.println("Connect Fail");
			return;
		}
		insertLog(scp.getLocalPath(), "ER");
		try {
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void insertLog(String folder, String file_status) {
		File fd = new File(folder);
		if (!fd.exists())
			return;
		StringBuilder value = new StringBuilder();
		File[] listFile = fd.listFiles();
		for (File file : listFile) {
			if (file.getPath().endsWith(EXT_EXCEL) || file.getPath().endsWith(EXT_TEXT)
					|| file.getPath().endsWith(EXT_CSV)) {
				value.append("('" + file.getName() + "'");
				value.append("," + CONFIG_ID);
				value.append(",'" + file_status + "'");
				value.append("," + 0);
				value.append(",NOW())");
				try {
					ControlDB.insertValues(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
							ControlDB.CONTROL_DB_PASS, "Logs", COLUMN_LIST_LOGS, value.toString());
					value = new StringBuilder();
				} catch (SQLException e) {
					e.printStackTrace();
				}
//				moveFile(file, IMPORT_DIR);
			}
		}
	}

	// II funcCheckFileStatus -> extract
	public void checkFileStatus() {
		ResultSet allRecordLogs = ControlDB.selectAllField(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
				ControlDB.CONTROL_DB_PASS, "LOGS");
		try {
			File file = null;
			String file_name = null;
			String file_status = null;
			int file_id = -999;
			String extention;
			while (allRecordLogs.next()) {
				file_name = allRecordLogs.getString("file_name");
				file_status = allRecordLogs.getString("file_status");
				file_id = allRecordLogs.getInt("id");
				if (file_status.equals("ER")) {
					String values = "";
					// Tien hanh ghi toàn bộ nội dung của file đó vào table student (trong DB
					// db_staging)
					// -> đồng thời chuyển trạng thái file đó thành TR
					file = new File(IMPORT_DIR + File.separator + file_name);
					extention = file.getPath().endsWith(".xlsx") ? EXT_EXCEL
							: file.getPath().endsWith(".txt") ? EXT_TEXT : EXT_CSV;
					if (!file.exists())
						break;
					StringTokenizer count_Field = new StringTokenizer(COLUMN_LIST, DELIM);
					if (file.getPath().endsWith(EXT_EXCEL)) {
						values = d_process.readValuesXLSX(file, file_id, count_Field.countTokens());
					} else if (file.getPath().endsWith(EXT_TEXT)) {
						values = d_process.readValuesTXT(file, file_id, count_Field.countTokens());
					} else if (file.getPath().endsWith(EXT_CSV)) {
						// Tu Tu lam
					}
					try {
						// extract to db_staging
						if (ControlDB.insertValues(STAGING_DB_NAME, STAGING_USER, STAGING_PASS, STAGING_TABLE,
								COLUMN_LIST + ",id_log", values)) {
							// change status in table logs
							ControlDB.updateFileStatus(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
									ControlDB.CONTROL_DB_PASS, file_id, "TR");
							ControlDB.updateCountLines(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
									ControlDB.CONTROL_DB_PASS, file_id, countLines(file, extention));

							/// transform data
							tranformData(allRecordLogs, file);

						}
					} catch (SQLException e) {
						ControlDB.updateFileStatus(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
								ControlDB.CONTROL_DB_PASS, file_id, "ERR");
						moveFile(file, ER_DIR);
						continue;
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				allRecordLogs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
//		 truncate table sinhvien in DBStaging if we had changed data
		try {
			ControlDB.truncateTable(ControlDB.CONTROL_DB_NAME_STAGING, ControlDB.CONTROL_DB_USER,
					ControlDB.CONTROL_DB_PASS, "student");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void tranformData(ResultSet allRecordLogs, File file) throws NumberFormatException, SQLException {
		ResultSet allValueDB_Staging = ControlDB.selectAllField(ControlDB.CONTROL_DB_NAME_STAGING,
				ControlDB.CONTROL_DB_USER, ControlDB.CONTROL_DB_PASS, "student");
		Student stu = new Student();
		String regex_dob = "\\d{4}[\\/\\-](0?[1-9]|1[012])[\\/\\-](0?[1-9]|[12][0-9]|3[01])*";
		while (allValueDB_Staging.next()) {
			String ngaySinh = allValueDB_Staging.getString("ngay_sinh");
			if (!Pattern.matches(regex_dob, ngaySinh))
				continue;
			int stt = Integer.parseInt(allValueDB_Staging.getString("stt"));
			String mssv = allValueDB_Staging.getString("mssv");
			String ho = allValueDB_Staging.getString("ho");
			String ten = allValueDB_Staging.getString("ten");
			String maLop = allValueDB_Staging.getString("ma_lop");
			String tenLop = allValueDB_Staging.getString("ten_lop");
			String sdt = allValueDB_Staging.getString("sdt");
			String email = allValueDB_Staging.getString("email");
			String queQuan = allValueDB_Staging.getString("que_quan");
			String ghiChu = allValueDB_Staging.getString("ghi_chu");
//			String date_expired = allValueDB_Staging.getString("date_expired");

			// check in DBWareHouse, If value duplicate
			if (ControlDB.selectOneField(ControlDB.CONTROL_DB_NAME_WAREHOUSE, ControlDB.CONTROL_DB_USER,
					ControlDB.CONTROL_DB_PASS, "warehouse", "mssv", "mssv", mssv) != null) {
			}
			stu.setStt(stt);
			stu.setMssv(mssv);
			stu.setHo(ho);
			stu.setTen(ten);
			stu.setNgaySinh(ngaySinh);
			stu.setMaLop(maLop);
			stu.setTenLop(tenLop);
			stu.setSdt(sdt);
			stu.setEmail(email);
			stu.setQueQuan(queQuan);
			stu.setGhiChu(ghiChu);
//				stu.setExpired(date_expired);
			try {
				// check insert data to DBStaging from DBWareHouse
				if (ControlDB.insertValuesDBStagingToDBWareHouse(ControlDB.CONTROL_DB_NAME_WAREHOUSE,
						ControlDB.CONTROL_DB_USER, ControlDB.CONTROL_DB_PASS, "student", COLUMN_LIST, stu)) {
					// Update log when insert data success
					ControlDB.updateLogs(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
							ControlDB.CONTROL_DB_PASS, allRecordLogs.getInt("id"), "SU");

				}
			} catch (Exception e) {
				System.out.println(file);
				System.out.println(e + "error");
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