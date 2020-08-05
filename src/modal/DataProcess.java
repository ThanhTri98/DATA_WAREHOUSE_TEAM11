package modal;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import util.ConnectionDB;

public class DataProcess {
	/*
	 * static được sử dụng để quản lý bộ nhớ tốt hơn và nó có thể được truy cập trực
	 * // tiếp thông qua lớp mà không cần khởi tạo.
	 */

	public static final String DELIM_COUNT_LINES = "$&$";
	private boolean dropFirstField = false;

	private String readLines(String value, String delim) {
		// Fix lỗi lỗi cú pháp sql
		// VD:Before: value = ABC|465|XX(YY)| -> After = ("ABC","456","XX(YY)") !!Error
		// Replace: -> ("ABC","456","XX'YY'") OK
		value = value.replaceAll("[()]", "'");
		String values = "(";
		StringTokenizer stoken = new StringTokenizer(value, delim);
		if (this.dropFirstField) {
			stoken.nextToken(); // Bỏ Field STT trong file
		}
		// Tổng số field trong đoạn value dựa vào delim
		int countToken = stoken.countTokens();
		String token = "";
		for (int i = 0; i < countToken; i++) {
			token = stoken.nextToken();
			// Nếu là field cuối cùng thì cộng thêm dấu )
			values += (i == countToken - 1) ? '"' + token.trim() + '"' + ")," : '"' + token.trim() + '"' + ",";
		}
		return values;
	}

	public String readValuesTXT(File s_file, String column_list) {
		if (!s_file.exists()) {
			return null;
		}
		int count_field = new StringTokenizer(column_list, ",").countTokens();
		int countLines = 0;
		String values = "";
		String delim = "|"; // hoặc \t
		try {
			BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(s_file), "utf8"));
			String line = bReader.readLine();
			if (line.indexOf("\t") != -1) {
				delim = "\t";
			}
			// Kiểm tra xem tổng số field trong file có đúng format
			if (!column_list.split(",")[0].equalsIgnoreCase(("STT"))) {
				count_field++;
			} else {
				this.dropFirstField = false;
			}
			if (new StringTokenizer(line, delim).countTokens() != count_field) {
				bReader.close();
				return null;
			}
			// STT|Mã sinh viên|Họ lót|Tên|...-> line.split(delim)[0]="STT" không phải số
			// nên là header -> bỏ qua line
			if (Pattern.matches("^[0-9]+$", line.split(delim)[0])) { // Kiem tra xem co phan header khong
				values += readLines(line, delim);
				countLines++;
			}
			while ((line = bReader.readLine()) != null) {
				// line = 1|17130005|Đào Thị Kim|Anh|15-08-1999|DH17DTB|Công nghệ thông tin
				// b|0123456789|17130005st@hcmuaf.edu.vn|Bến Tre|abc
				// line + " " + delim = 1|17130005|Đào Thị Kim|Anh|15-08-1999|DH17DTB|Công nghệ
				// thông tin b|0123456789|17130005st@hcmuaf.edu.vn|Bến Tre|abc |
				// Nếu có field 11 thì dư khoảng trắng lên readLines() có trim(), còn 10 field
				// thì fix lỗi out index
				values += readLines(line + " ", delim);
				countLines++;
			}
			bReader.close();
			return countLines + DELIM_COUNT_LINES + values.substring(0, values.length() - 1);

		} catch (NoSuchElementException | IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public String readValuesXLSX(File s_file, String column_list) {
		if (!s_file.exists() || !s_file.isFile()) { // OUT nếu s_file không tồn tại hoặc là folder
			SendMail.writeLogsToLocalFile(" --> FILE NOT FOUND!!!: " + s_file.getName());
			return null;
		}
		String values = "";
		String value = "";
		String delim_xlsx = "|";
		// Tổng số field trong column_list + 1 ( +1 cho cột STT)
		int totalCells = new StringTokenizer(column_list, ",").countTokens() + 1;
		int totalRows = 0;
		int countLines = 0;
		int firstRow = 1;
		FileInputStream fileIn = null;
		XSSFWorkbook workBooks = null;
		try {
			fileIn = new FileInputStream(s_file);
			workBooks = new XSSFWorkbook(fileIn);
			XSSFSheet sheet = workBooks.getSheetAt(0);
			totalRows = sheet.getLastRowNum();
			Row rowCheck = sheet.getRow(0); // Lấy ra hàng đầu tiên
			// Kiem tra xem file co dung format hay chua dua vao so cell trong file
			if (rowCheck.getLastCellNum() < totalCells - 1 || rowCheck.getLastCellNum() > totalCells) {
				SendMail.writeLogsToLocalFile(" -> FILE NOT FORMATED!!!: " + s_file.getName());
				workBooks.close();
				return null;
			}
			// Kiểm tra xem row đầu tiên có phải là header không?
			if (rowCheck.cellIterator().next().getCellType().equals(CellType.NUMERIC)) {
				firstRow = 0; // default = 1, nếu không phải là header thì row bắt đầu lại từ 0
			}
			for (int rw = firstRow; rw < totalRows; rw++) {
				Row row = sheet.getRow(rw); // Lấy ra row 'rw'
				if (row != null) {
					int count_empty = 0;
					for (int ce = 1; ce < totalCells; ce++) {
						Cell cell = row.getCell(ce); // Lấy ra cell 'ce' trong row 'rw'
						if (cell != null) {
							switch (cell.getCellType()) {
							case NUMERIC: // dạng số
								if (DateUtil.isCellDateFormatted(cell)) { // Nếu thuộc định dạng ngày
									SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
									value += dateFormat.format(cell.getDateCellValue()) + delim_xlsx;
								} else {
									value += (long) cell.getNumericCellValue() + delim_xlsx;
								}
								break;
							case STRING: // Dạng chuỗi
								value += cell.getStringCellValue() + delim_xlsx;
								break;
							case FORMULA: // Ô công thức
								switch (cell.getCachedFormulaResultType()) {
								case STRING:
									value += cell.getStringCellValue() + delim_xlsx;
									break;
								default:
									value += "str_empty" + delim_xlsx;
									count_empty++;
									break;
								}
								break;
							case BLANK:
							default:
								value += "str_empty" + delim_xlsx;
								count_empty++;
								break;
							}
						} else { // Cell này bị null (Blank)
							value += "str_empty" + delim_xlsx;
							count_empty++;
						}
					}
					if (count_empty != totalCells - 1) {
						values += readLines(value, delim_xlsx);
						countLines++;
					}
					value = "";
				}
			}
			return countLines + DELIM_COUNT_LINES + values.substring(0, values.length() - 1);
		} catch (IOException e) {
			return null;
		} finally {
			try {
				if (workBooks != null)
					workBooks.close();
				if (fileIn != null)
					fileIn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void moveFile(File file, String target_dir) {
		BufferedInputStream bReader = null;
		BufferedOutputStream bWriter = null;
		try {
			bReader = new BufferedInputStream(new FileInputStream(file));
			bWriter = new BufferedOutputStream(new FileOutputStream(target_dir + File.separator + file.getName()));
			byte[] buff = new byte[1024 * 10];
			int data = 0;
			while ((data = bReader.read(buff)) != -1) {
				bWriter.write(buff, 0, data);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bReader != null)
					bReader.close();
				if (bWriter != null)
					bWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			file.delete();
		}
	}

	private String convertDate(String date) { // dd-/MM-/yyyy >>> yyyy-MM-dd
		String result = "";
		String delim = "/";
		if (date.indexOf("-") != -1) {
			delim = "-";
		}
		String[] dateArr = date.split(delim);
		// 5/-5/-2525 -> 05-05-2525
		for (int i = 0; i < dateArr.length - 1; i++) {
			if (dateArr[i].length() == 1) {
				dateArr[i] = "0" + dateArr[i];
			}
		}
		result = dateArr[2] + "-" + dateArr[1] + "-" + dateArr[0];

		return result;
	}

	public static void main(String[] args) {
		System.out.println(new DataProcess().readValuesXLSX(
				new File("C:\\WAREHOUSE\\SUCCESS_DIR\\STUDENT\\sinhvien_chieu_nhom3.xlsx"),
				"mssv,ho,ten,ngay_sinh,ma_lop,ten_lop,sdt,email,que_quan,ghi_chu"));
	}

	public int transferData(ResultSet data_staging, int id_log, String column_list, String process_function) {
	return 0;
	}
}
