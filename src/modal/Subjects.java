package modal;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Subjects {
	private int id;
	private int stt;
	private int ma_mh;
	private String ten_mh;
	private int tin_chi;
	private String khoa_bm_ql;
	private String khoa_bm_sd;

	public Subjects(int stt, int ma_mh, String ten_mh, int tin_chi, String khoa_bm_ql, String khoa_bm_sd) {
		this.stt = stt;
		this.ma_mh = ma_mh;
		this.ten_mh = ten_mh;
		this.tin_chi = tin_chi;
		this.khoa_bm_ql = khoa_bm_ql;
		this.khoa_bm_sd = khoa_bm_sd;
	}

	public Subjects() {
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getStt() {
		return stt;
	}

	public void setStt(int stt) {
		this.stt = stt;
	}

	public int getMa_mh() {
		return ma_mh;
	}

	public void setMa_mh(int ma_mh) {
		this.ma_mh = ma_mh;
	}

	public String getTen_mh() {
		return ten_mh;
	}

	public void setTen_mh(String ten_mh) {
		this.ten_mh = ten_mh;
	}

	public int getTin_chi() {
		return tin_chi;
	}

	public void setTin_chi(int tin_chi) {
		this.tin_chi = tin_chi;
	}

	public String getKhoa_bm_ql() {
		return khoa_bm_ql;
	}

	public void setKhoa_bm_ql(String khoa_bm_ql) {
		this.khoa_bm_ql = khoa_bm_ql;
	}

	public String getKhoa_bm_sd() {
		return khoa_bm_sd;
	}

	public void setKhoa_bm_sd(String khoa_bm_sd) {
		this.khoa_bm_sd = khoa_bm_sd;
	}

	@Override
	public String toString() {
		return stt + "|" + ma_mh + "|" + ten_mh + "|" + tin_chi + "|" + khoa_bm_ql + "|" + khoa_bm_sd;
	}

	public Subjects getSubjects(ResultSet rs) {
		try {
			Subjects sub = new Subjects();
			sub.setId(rs.getInt("id"));
			sub.setStt(rs.getInt("stt"));
			sub.setMa_mh(rs.getInt("ma_mh"));
			sub.setTen_mh(rs.getString("ten_mh"));
			sub.setTin_chi(rs.getInt("tin_chi"));
			sub.setKhoa_bm_ql(rs.getString("khoa_bm_ql"));
			sub.setKhoa_bm_sd(rs.getString("khoa_bm_sd"));
			return sub;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}

	}

}
