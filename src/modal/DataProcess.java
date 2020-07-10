package modal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import dao.ControlDB;

public class DataProcess {
	static final String NUMBER_REGEX = "^[0-9]+$";
	static final String DATE_FORMAT = "yyyy-MM-dd";

	private String readLines(String value, String delim) {
		String values = "";
		StringTokenizer stoken = new StringTokenizer(value, delim);
		int countToken = stoken.countTokens();
		String lines = "(";
		String token = "";
		for (int j = 0; j < countToken; j++) {
			token = stoken.nextToken();
			lines += (j == countToken - 1) ? '"' + token.trim() + '"' + ")," : '"' + token.trim() + '"' + ",";
			values += lines;
			lines = "";
		}
		return values;
	}

	public String readValuesTXT(File s_file, int id_log, int count_field) {
		if (!s_file.exists()) {
			return null;
		}
		String values = "";
		String delim = "|"; // hoặc \t
		try {
			BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(s_file)));
			String line = bReader.readLine();
			if (line.indexOf("\t") != -1) {
				delim = "\t";
			}
			// Kiểm tra xem tổng số trường trong file có đúng format
			if (new StringTokenizer(line, delim).countTokens() != count_field) {
				bReader.close();
				return null;
			}
			if (Pattern.matches(NUMBER_REGEX, line.split(delim)[0])) { // Kiem tra xem co phan header khong
				values += readLines(line + delim + id_log, delim);
			}
			while ((line = bReader.readLine()) != null) {
//				System.out.println(line +"2"+ delim + id_log);
				values += readLines(line + " " + delim + id_log, delim);
			}
			bReader.close();
			return values.substring(0, values.length() - 1);

		} catch (NoSuchElementException | IOException e) {
			e.printStackTrace();
			return null;
		}
	}


	public String readValuesXLSX(File s_file, int id_log, int countCell) {
		String values = "";
		String value = "";
		String delim_xlsx = "|";
		try {
			FileInputStream fileIn = new FileInputStream(s_file);
			XSSFWorkbook workBooks = new XSSFWorkbook(fileIn);
			XSSFSheet sheet = workBooks.getSheetAt(0);
			Iterator<Row> rows = sheet.iterator();
			if (rows.next().cellIterator().next().getCellType().equals(CellType.NUMERIC)) { // Kiem tra xem co phan
																							// header khong
				rows = sheet.iterator();// vi goi rows.next thi cur index =1, neu khong co header thi set lại cur index
										// =0
			}
			while (rows.hasNext()) {
				Row row = rows.next();
				// Kiem tra xem file co dung format hay chua dua vao so cell trong file
				if (row.getLastCellNum() < countCell - 1 || row.getLastCellNum() > countCell) {
					workBooks.close();
					return null;
				}
				Iterator<Cell> cells = row.cellIterator();
				while (cells.hasNext()) {
					Cell cell = cells.next();
					CellType cellType = cell.getCellType();
					switch (cellType) {
					case NUMERIC:
						if (DateUtil.isCellDateFormatted(cell)) {
							SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
							value += dateFormat.format(cell.getDateCellValue()) + delim_xlsx;
						} else {
							value += (long) cell.getNumericCellValue() + delim_xlsx;
						}
						break;
					case STRING:
						value += cell.getStringCellValue() + delim_xlsx;
						break;
					case FORMULA:
						switch (cell.getCachedFormulaResultType()) {
						case NUMERIC:
							value += (long) cell.getNumericCellValue() + delim_xlsx;
							break;
						case STRING:
							value += cell.getStringCellValue() + delim_xlsx;
							break;
						default:
							value += " " + delim_xlsx;
							break;
						}
						break;
					case BLANK:
					default:
						value += " " + delim_xlsx;
						break;
					}
				}
				if (row.getLastCellNum() == countCell - 1) {
					value += " |";
				}
				values += readLines(value + id_log, delim_xlsx);
				value = "";
			}
			workBooks.close();
			fileIn.close();
			return values.substring(0, values.length() - 1);
		} catch (IOException e) {
			return null;
		}
	}

	public int transformData(ResultSet data_staging) {
		// Nếu trùng mssv thì insert dòng mới và update time_expire là NOW()
		//
		String regex_dob_1 = "^\\d{4}[\\/\\-](0?[1-9]|1[012])[\\/\\-](0?[1-9]|[12][0-9]|3[01])+$";
		String regex_dob_2 = "^(0?[1-9]|[12][0-9]|3[01])+[\\/\\-](0?[1-9]|1[012])[\\/\\-]\\d{4}$";
		int count = 0;
		Student stu = new Student();
		try {
			while (data_staging.next()) {
				String ngay_sinh = data_staging.getString("ngay_sinh");
				// Kiểm tra định dạng ngày sinh yyyy/-MM/-dd or dd/-MM/-yyyy -> Đưa về định dạng
				// yyyy-MM-dd
				// Nếu khác thì bỏ qua bảng ghi này.
				if (!Pattern.matches(regex_dob_1, ngay_sinh) && !Pattern.matches(regex_dob_2, ngay_sinh)) {
					continue;
				}
				// Nếu là định dạng dd/-MM/-yyyy thì chuyển thành yyyy/-MM/-dd
				if (Pattern.matches(regex_dob_2, ngay_sinh)) {

				}
				int stt = Integer.parseInt(data_staging.getString("stt"));
				String mssv = data_staging.getString("mssv");
				String ho = data_staging.getString("ho");
				String ten = data_staging.getString("ten");
				String ma_lop = data_staging.getString("ma_lop");
				String ten_lop = data_staging.getString("ten_lop");
				String sdt = data_staging.getString("sdt");
				String email = data_staging.getString("email");
				String que_quan = data_staging.getString("que_quan");
				String ghi_chu = data_staging.getString("ghi_chu");
				int id_log = data_staging.getInt("id_log");
				String time_expire = "NOW()";

				// check in DBWareHouse, If value duplicate
				if (ControlDB.selectOneField(DataWarehouse.W_DB_NAME, DataWarehouse.W_USER, DataWarehouse.W_PASS,
						"student", "mssv", "mssv", mssv) != null) {
					continue;
				}
				stu.setStt(stt);
				stu.setMssv(mssv);
				stu.setHo(ho);
				stu.setTen(ten);
				stu.setNgaySinh(ngay_sinh);
				stu.setMaLop(ma_lop);
				stu.setTenLop(ten_lop);
				stu.setSdt(sdt);
				stu.setEmail(email);
				stu.setQueQuan(que_quan);
				stu.setGhiChu(ghi_chu);
				String columnList = DataWarehouse.COLUMN_LIST + "," + "id_log" + "," + "time_expire";
				try {
					// check insert data to DBStaging from DBWareHouse
					if (ControlDB.insertValuesDBStagingToDBWareHouse(DataWarehouse.W_DB_NAME, DataWarehouse.W_USER,
							DataWarehouse.W_PASS, "student", columnList, stu, id_log, time_expire)) {
						count++;

					}
					
				} catch (Exception e) {
					System.out.println(e + "error");
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
//		 truncate table sinhvien in DBStaging if we had changed data
		try {
			ControlDB.truncateTable(DataWarehouse.STAGING_DB_NAME, DataWarehouse.STAGING_USER,
					DataWarehouse.STAGING_PASS, DataWarehouse.STAGING_TABLE);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return count;
	}
}
