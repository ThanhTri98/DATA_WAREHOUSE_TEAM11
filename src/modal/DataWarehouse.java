package modal;

import dao.ControlDB;

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
	//II funcCheckFileStatus -> extract
	
}