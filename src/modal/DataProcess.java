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
	public static final String NUMBER_REGEX = "^[0-9]+$";
	public static final String DATE_FORMAT = "yyyy-MM-dd";

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
			BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(s_file),"utf8"));
			String line = bReader.readLine();
			if (line.indexOf("\t") != -1) {
				delim = "\t";
			}
			// Kiểm tra xem tổng số field trong file có đúng format
			if (new StringTokenizer(line, delim).countTokens() != count_field) {
				bReader.close();
				return null;
			}
			// STT|Mã sinh viên|Họ lót|Tên|...-> line.split(delim)[0]="STT" không phải số
			// nên là header -> bỏ qua line
			if (Pattern.matches(NUMBER_REGEX, line.split(delim)[0])) { // Kiem tra xem co phan header khong
				values += readLines(line + delim + id_log, delim);
			}
			while ((line = bReader.readLine()) != null) {
				// line = 1|17130005|Đào Thị Kim|Anh|15-08-1999|DH17DTB|Công nghệ thông tin
				// b|0123456789|17130005st@hcmuaf.edu.vn|Bến Tre|abc
				// line + " " + delim = 1|17130005|Đào Thị Kim|Anh|15-08-1999|DH17DTB|Công nghệ
				// thông tin b|0123456789|17130005st@hcmuaf.edu.vn|Bến Tre|abc |
				// Nếu có field 11 thì dư khoảng trắng lên readLines() có trim(), còn 10 field
				// thì fix lỗi out index
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

	public boolean moveFile(File file, String target_dir) {
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

	public int countLines(File file, String extention) {
		int result = 0;
		XSSFWorkbook workBooks = null;
		try {
			if (extention.indexOf(".txt") != -1) {
				BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(file),"utf8"));
				String line;
				while ((line = bReader.readLine()) != null) {
					if (!line.trim().isEmpty()) {
						result++;
					}
				}
				bReader.close();
			} else if (extention.indexOf(".xlsx") != -1) {
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

	private String convertDate(String date) {
		String result = "";
		String delim = "/";
		if (date.indexOf("-") != -1) {
			delim = "-";
		}
		String[] dateArr = date.split(delim);
//		if (dateArr[0].length() == 4 && delim == "/") { // 2525/05/05 ?? 2525/5/5
//			// 5/-5/-2525 -> 05/05/2525
//			for (int i = 1; i < dateArr.length; i++) {
//				if (dateArr[i].length() == 1) {
//					dateArr[i] = "0" + dateArr[i];
//				}
//			}
//			result = dateArr[0] + "/" + dateArr[1] + "/" + dateArr[2];
//		} else if (dateArr[0].length() == 4 && delim == "-") { // 2525-05-05 -> 2525/05/05
//			for (int i = 1; i < dateArr.length; i++) {
//				if (dateArr[i].length() == 1) {
//					dateArr[i] = "0" + dateArr[i];
//				}
//			}
//			result = dateArr[0] + "/" + dateArr[1] + "/" + dateArr[2];
//		}
			// 5/-5/-2525 -> 05/05/2525
			for (int i = 0; i < dateArr.length - 1; i++) {
				if (dateArr[i].length() == 1) {
					dateArr[i] = "0" + dateArr[i];
				}
			}
			result = dateArr[2] + "/" + dateArr[1] + "/" + dateArr[0];

		return result;
	}

	public int transformData(ResultSet data_staging,int id_log) {
		// Nếu trùng mssv thì insert dòng mới và update time_expire là NOW()
		//
		String regex_date_1 = "^\\d{4}[\\/\\-](0?[1-9]|1[012])[\\/\\-](0?[1-9]|[12][0-9]|3[01])+$";
		String regex_date_2 = "^(0?[1-9]|[12][0-9]|3[01])+[\\/\\-](0?[1-9]|1[012])[\\/\\-]\\d{4}$";
		int count = 0;
		Student stu = new Student();
		try {
			while (data_staging.next()) {
				String ngay_sinh = data_staging.getString("ngay_sinh");
				// Kiểm tra định dạng ngày sinh yyyy/-MM/-dd or dd/-MM/-yyyy -> Đưa về định dạng
				// yyyy/MM/dd
				// Nếu khác thì bỏ qua bảng ghi này.
				if (!Pattern.matches(regex_date_1, ngay_sinh) && !Pattern.matches(regex_date_2, ngay_sinh)) {
					continue;
				}
				// Nếu là định dạng dd/-MM/-yyyy or yyyy-MM-dd thì chuyển thành yyyy/MM/dd
				ngay_sinh = convertDate(ngay_sinh);
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

				// check in DBWareHouse, If value duplicate
//				if (ControlDB.selectOneField(DataWarehouse.W_DB_NAME, DataWarehouse.W_USER, DataWarehouse.W_PASS,
//						"student", "mssv", "mssv", mssv) != null) {
//					System.out.println("Duplicate mssv");
//					continue;
//				}
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
				stu.setGhiChu(ghi_chu.length() == 0 ? "str_empty" : ghi_chu);
				String columnList = DataWarehouse.COLUMN_LIST + "," + "id_log" + "," + "time_expire";
				// check insert data to DBStaging from DBWareHouse
				if (ControlDB.insertValuesDBStagingToDBWareHouse(DataWarehouse.W_DB_NAME, DataWarehouse.W_USER,
						DataWarehouse.W_PASS, "SINHVIEN", columnList, stu, id_log)) {
					count++;
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		ControlDB.truncateTable(DataWarehouse.STAGING_DB_NAME, DataWarehouse.STAGING_USER, DataWarehouse.STAGING_PASS,
				DataWarehouse.STAGING_TABLE);

		return count;
	}
}
