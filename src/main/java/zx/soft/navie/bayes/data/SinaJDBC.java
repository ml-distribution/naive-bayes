package zx.soft.navie.bayes.data;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.navie.bayes.utils.ConfigUtil;

/**
 * 舆情数据JDBC
 * @author wanggang
 *
 */
public class SinaJDBC {

	private static Logger logger = LoggerFactory.getLogger(SinaJDBC.class);

	private BasicDataSource dataSource;

	private String db_url;
	private String db_username;
	private String db_password;

	public SinaJDBC() {
		dbInit();
		dbConnection();
	}

	/**
	 * 初始化数据库相关参数
	 */
	private void dbInit() {
		Properties props = ConfigUtil.getProps("data_db.properties");
		db_url = props.getProperty("sent.db.url");
		db_username = props.getProperty("sent.db.username");
		db_password = props.getProperty("sent.db.password");
	}

	/**
	 * 链接数据库
	 */
	private void dbConnection() {
		dataSource = new BasicDataSource();
		dataSource.setDriverClassName("com.mysql.jdbc.Driver");
		dataSource.setUrl(db_url);
		dataSource.setUsername(db_username);
		dataSource.setPassword(db_password);
		dataSource.setTestOnBorrow(true);
		dataSource.setValidationQuery("select 1");
	}

	/**
	 * 获取链接
	 */
	private Connection getConnection() {
		try {
			return dataSource.getConnection();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 关闭数据库
	 */
	public void dbClose() {
		try {
			dataSource.close();
		} catch (SQLException e) {
			logger.info("Db close error.");
		}
	}

	@Override
	public String toString() {
		return "SentJDBC:[db_url=" + db_url + ",db_username=" + db_username + ",db_password=" + db_password + "]";
	}

}
