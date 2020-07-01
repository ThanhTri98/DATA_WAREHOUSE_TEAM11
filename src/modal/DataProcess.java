package modal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
				values += readLines(line +" "+ delim + id_log, delim);
			}
			bReader.close();
			return values.substring(0, values.length() - 1);

		} catch (NoSuchElementException | IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void main(String[] args) {
		DataProcess dp = new DataProcess();
		System.out.println(dp.readValuesTXT(new File("C:\\WAREHOUSE\\ERROR_DIR\\sinhvien_sang_nhom1.txt"), 1, 11));
//		System.out.println(dp.readValuesXLSX(new File("C:\\WAREHOUSE\\IMPORT_DIR\\sinhvien_sang_nhom11.xlsx"),1,11));
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

}
