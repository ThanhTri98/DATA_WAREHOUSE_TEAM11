package modal;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Student {
	private int stt;
	private String mssv;
	private String ho;
	private String ten;
	private String ngaySinh;
	private String maLop;
	private String tenLop;
	private String sdt;
	private String email;
	private String queQuan;
	private String ghiChu;

	public int getStt() {
		return stt;
	}

	public void setStt(int stt) {
		this.stt = stt;
	}

	public String getMssv() {
		return mssv;
	}

	public void setMssv(String mssv) {
		this.mssv = mssv;
	}

	public String getHo() {
		return ho;
	}

	public void setHo(String ho) {
		this.ho = ho;
	}

	public String getTen() {
		return ten;
	}

	public void setTen(String ten) {
		this.ten = ten;
	}

	public String getNgaySinh() {
		return ngaySinh;
	}

	public void setNgaySinh(String ngaySinh) {
		this.ngaySinh = ngaySinh;
	}

	public String getMaLop() {
		return maLop;
	}

	public void setMaLop(String maLop) {
		this.maLop = maLop;
	}

	public String getTenLop() {
		return tenLop;
	}

	public void setTenLop(String tenLop) {
		this.tenLop = tenLop;
	}

	public String getSdt() {
		return sdt;
	}

	public void setSdt(String sdt) {
		this.sdt = sdt;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getQueQuan() {
		return queQuan;
	}

	public void setQueQuan(String queQuan) {
		this.queQuan = queQuan;
	}

	public String getGhiChu() {
		return ghiChu;
	}

	public void setGhiChu(String ghiChu) {
		this.ghiChu = ghiChu;
	}

	public Student getStudent(ResultSet rs) {
		Student stu = null;
		try {
			rs.next();
			stu = new Student();
			stu.setStt(Integer.parseInt(rs.getString("stt")));
			stu.setMssv(rs.getString("mssv"));
			stu.setHo(rs.getString("ho"));
			stu.setTen(rs.getString("ten"));
			stu.setNgaySinh(rs.getString("ngay_sinh"));
			stu.setMaLop(rs.getString("ma_lop"));
			stu.setTen(rs.getString("ten_lop"));
			stu.setSdt(rs.getString("sdt"));
			stu.setEmail(rs.getString("email"));
			stu.setQueQuan(rs.getString("que_quan"));
			stu.setGhiChu(rs.getString("ghi_chu"));
			return stu;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

}
