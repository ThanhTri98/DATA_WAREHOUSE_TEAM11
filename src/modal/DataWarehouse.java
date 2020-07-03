package modal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.StringTokenizer;

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

	DataProcess d_process;

	public DataWarehouse() {
		d_process = new DataProcess();
	}
	public static void main(String[] args) {
		DataWarehouse d_warehouse = new DataWarehouse();
	}

	/*
	 * I. Táº£i file vá»� thÆ° má»¥c C:\WAREHOUSE\SCP dÃ¹ng SCP ### 1. Táº£i hoÃ n táº¥t thÃ¬ quÃ©t
	 * thÆ° má»¥c SCP vÃ  ghi log -> file_status = ER -> Move file vá»«a ghi log vÃ o
	 * C:\WAREHOUSE\IMPORT_DIR, Kiá»ƒm tra file Ä‘Ã³ Ä‘Ã£ Ä‘Æ°á»£c import vÃ o há»‡ thá»‘ng
	 * chÆ°a(file_status=TR,SU), náº¿u tá»“n táº¡i thÃ¬ khÃ´ng táº£i ná»¯a ### 2. VÃ o báº£ng Logs
	 * (trong DB control_db) Ä‘á»�c táº¥t cáº£ records, náº¿u rcd Ä‘Ã³ cÃ³ file_status = ER thÃ¬
	 * ghi toÃ n bá»™ ná»™i dung cá»§a file Ä‘Ã³ vÃ o table student (trong DB db_staging) ->
	 * Ä‘á»“ng thá»�i chuyá»ƒn tráº¡ng thÃ¡i file Ä‘Ã³ thÃ nh TR
	 */
	/*
	 * II. Tiáº¿n hÃ nh tranform dá»¯ liá»‡u ### 1. VÃ o báº£ng Logs (trong DB control_db) Ä‘á»�c
	 * táº¥t cáº£ records, náº¿u rcd Ä‘Ã³ cÃ³ file_status = TR -> vÃ o báº£ng student (trong DB
	 * db_staging) Ä‘á»�c táº¥t cáº£ cÃ¡c rcd cÃ³ trÆ°á»�ng file_name = file hiá»‡n táº¡i cÃ³
	 * file_status = TR xong thÃ¬ tiáº¿n hÃ ng transform dá»¯ liá»‡u ### 2. Sau khi Ä‘Ã£
	 * tranform táº¥t cáº£ cÃ¡c dÃ²ng trong file thÃ¬ lÆ°u láº¡i sá»‘ dÃ²ng Ä‘Ã£ trans, náº¿u sá»‘ dÃ²ng
	 * Ä‘Ã£ trans = sá»‘ dÃ²ng Ä‘á»�c lÃªn tá»« file thÃ¬ chuyá»ƒn tráº¡ng thÃ¡i file Ä‘Ã³ thÃ nh SU,
	 * ngÆ°á»£c láº¡i thÃ¬ ERR -> Move cÃ¡c file vÃ o C:\WAREHOUSE\ERROR_DIR???
	 */
	/*
	 * III. Tiáº¿n hÃ nh ghi cÃ¡c file cÃ³ file_status = SU vÃ o báº£ng student trong DB
	 * warehouse ### 1. VÃ o báº£ng Logs (trong DB control_db) Ä‘á»�c táº¥t cáº£ cÃ¡c records,
	 * náº¿u rcd nÃ o cÃ³ file_status = SU thÃ¬ tiáº¿n hÃ nh move dá»¯ liá»‡u tá»« báº£ng student
	 * (trong DB db_staging) qua báº£ng student (trong DB warehouse) ### 2. QuÃ¡ trÃ¬nh
	 * di chuyá»ƒn hoÃ n táº¥t -> Move cÃ¡c file Ä‘Ã³ vÃ o thÆ° má»¥c C:\WAREHOUSE\SUCCESS_DIR
	 * -> Káº¿t thÃºc chu trÃ¬nh.
	 * 
	 * -------Náº¿u file_status = ERR thÃ¬ lÃ m láº¡i B1 nhÆ°ng thay thÆ° má»¥c SCP thÃ nh
	 * C:\WAREHOUSE\ERROR_DIR
	 */

//I. funcDownload, funcInsertLog
	public void downloadFile() {
		ResultSet rs = ControlDB.selectAllField(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
				ControlDB.CONTROL_DB_PASS, "scp");
		SCP scp = new SCP().getSCP(rs);
		FileDownLib.fileDownload(scp.getHostName(), scp.getPort(), scp.getUserName(), scp.getPassword(),
				scp.getRemotePath(), scp.getLocalPath(), scp.getSyncMustMatch());
		System.out.println("DownFile success.");
		// Kiểm tra file down xuống có lỗi hay không
//		if () {}
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
		String column_list_log = "file_name,config_id,file_status,staging_load_count,file_timestamp";
		StringBuilder value = new StringBuilder();
		String extention;
		File[] listFile = fd.listFiles();
		for (File file : listFile) {
			if (file.getPath().endsWith(EXT_EXCEL) || file.getPath().endsWith(EXT_TEXT)
					|| file.getPath().endsWith(EXT_CSV)) {
				extention = file.getPath().endsWith(".xlsx") ? EXT_EXCEL
						: file.getPath().endsWith(".txt") ? EXT_TEXT : EXT_CSV;
				value.append("('" + file.getName() + "'");
				value.append("," + CONFIG_ID);
				value.append(",'" + file_status + "'");
				value.append("," + countLines(file, extention));
				value.append(",'" + ControlDB.dtf.format(ControlDB.now) + "')");
				try {
					ControlDB.insertValues(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
							ControlDB.CONTROL_DB_PASS, "Logs", column_list_log, value.toString());
					value = new StringBuilder();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	//II funcCheckFileStatus -> extract
	public void checkFileStatus() {
		ResultSet allRecordLogs = ControlDB.selectAllField(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
				ControlDB.CONTROL_DB_PASS, "logs");
		try {
			File file = null;
			String file_name=null;
			String file_status=null;
			int file_id=-999;
			String extention;
			while (allRecordLogs.next()) {
				file_name = allRecordLogs.getString("file_name");
				file_status=allRecordLogs.getString("file_status");
				file_id =allRecordLogs.getInt("id");
				if (file_status.equals("ER")) {
					String values = "";
					// Tien hanh ghi toÃ n bá»™ ná»™i dung cá»§a file Ä‘Ã³ vÃ o table student (trong DB
					// db_staging)
					// -> Ä‘á»“ng thá»�i chuyá»ƒn tráº¡ng thÃ¡i file Ä‘Ã³ thÃ nh TR
					file = new File(IMPORT_DIR + File.separator + file_name);
					extention = file.getPath().endsWith(".xlsx") ? EXT_EXCEL
							: file.getPath().endsWith(".txt") ? EXT_TEXT : EXT_CSV;
					if (!file.exists())
						break;
					if (file.getPath().endsWith(EXT_EXCEL)) {
						StringTokenizer str = new StringTokenizer(COLUMN_LIST, DELIM);
						values = d_process.readValuesXLSX(file, file.getName(), str.countTokens());
					} else if (file.getPath().endsWith(EXT_TEXT)) {
						values = d_process.readValuesTXT(file, DELIM, file.getName());
					} else if (file.getPath().endsWith(EXT_CSV)) {
						// Tu Tu lam
					}
					try {
						// extract to db_staging
						if (ControlDB.insertValues(STAGING_DB_NAME, STAGING_USER, STAGING_PASS, STAGING_TABLE,
								COLUMN_LIST + ",file_name", values)) {
							// change status in table logs
							ControlDB.updateFileStatus(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
									ControlDB.CONTROL_DB_PASS, file_id, "TR");
							ControlDB.updateCountLines(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
									ControlDB.CONTROL_DB_PASS, file_id, countLines(file, extention));
							
						}
					} catch (SQLException e) {
						ControlDB.updateFileStatus(ControlDB.CONTROL_DB_NAME, ControlDB.CONTROL_DB_USER,
								ControlDB.CONTROL_DB_PASS, file_id, "ERR");
						continue;
					}

				} else if (allRecordLogs.getString("file_status").equals("TR")) {

				} else if (allRecordLogs.getString("file_status").equals("SU")) {

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