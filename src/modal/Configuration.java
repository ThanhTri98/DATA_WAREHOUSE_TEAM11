package modal;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Configuration {
	private int idConfig;
	private String importDir;
	private String SuccessDir;
	private String errorDir;
	private String colmnList;
	private String delimiter;
	private String warehouseDBName;
	private String warehouseUser;
	private String warehousePass;
	private String warehouseTable;
	private String stagingDBName;
	private String stagingUser;
	private String stagingPass;
	private String stagingTable;

	public int getIdConfig() {
		return idConfig;
	}

	public void setIdConfig(int idConfig) {
		this.idConfig = idConfig;
	}

	public String getImportDir() {
		return importDir;
	}

	public void setImportDir(String importDir) {
		this.importDir = importDir;
	}

	public String getSuccessDir() {
		return SuccessDir;
	}

	public void setSuccessDir(String successDir) {
		SuccessDir = successDir;
	}

	public String getErrorDir() {
		return errorDir;
	}

	public void setErrorDir(String errorDir) {
		this.errorDir = errorDir;
	}

	public String getColmnList() {
		return colmnList;
	}

	public void setColmnList(String colmnList) {
		this.colmnList = colmnList;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public String getWarehouseDBName() {
		return warehouseDBName;
	}

	public void setWarehouseDBName(String warehouseDBName) {
		this.warehouseDBName = warehouseDBName;
	}

	public String getWarehouseUser() {
		return warehouseUser;
	}

	public void setWarehouseUser(String warehouseUser) {
		this.warehouseUser = warehouseUser;
	}

	public String getWarehousePass() {
		return warehousePass;
	}

	public void setWarehousePass(String warehousePass) {
		this.warehousePass = warehousePass;
	}

	public String getWarehouseTable() {
		return warehouseTable;
	}

	public void setWarehouseTable(String warehouseTable) {
		this.warehouseTable = warehouseTable;
	}

	public String getStagingDBName() {
		return stagingDBName;
	}

	public void setStagingDBName(String stagingDBName) {
		this.stagingDBName = stagingDBName;
	}

	public String getStagingUser() {
		return stagingUser;
	}

	public void setStagingUser(String stagingUser) {
		this.stagingUser = stagingUser;
	}

	public String getStagingPass() {
		return stagingPass;
	}

	public void setStagingPass(String stagingPass) {
		this.stagingPass = stagingPass;
	}

	public String getStagingTable() {
		return stagingTable;
	}

	public void setStagingTable(String stagingTable) {
		this.stagingTable = stagingTable;
	}

	public Configuration getConfiguration(ResultSet rs) {
		Configuration conf = null;
		try {
			rs.next();
			conf = new Configuration();
			conf.setIdConfig(rs.getInt("config_id"));
			conf.setImportDir(rs.getString("import_dir"));
			conf.setSuccessDir(rs.getString("success_dir"));
			conf.setErrorDir(rs.getString("error_dir"));
			conf.setColmnList(rs.getString("column_list"));
			conf.setDelimiter(rs.getString("delimiter"));
			conf.setWarehouseTable(rs.getString("warehouse_table"));
			conf.setStagingTable(rs.getString("staging_table"));
			conf.setWarehouseDBName(rs.getString("warehouse_db_name"));
			conf.setWarehouseUser(rs.getString("warehouse_user"));
			conf.setWarehousePass(rs.getString("warehouse_pass"));
			conf.setStagingDBName(rs.getString("staging_db_name"));
			conf.setStagingUser(rs.getString("staging_user"));
			conf.setStagingPass(rs.getString("staging_pass"));
			return conf;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

}
