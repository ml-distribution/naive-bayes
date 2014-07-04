package zx.soft.naive.bayes.mapred.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * 数据库中的数据模型。
 * 注意：这里仅代表数据表中的wid和text字段，如果还需要其他字段需要添加进来。
 * 
 * @author wanggang
 *
 */
public class DbInputWritable implements Writable, DBWritable {

	private long wid; // 微博ID
	private String text; // 微博内容

	@Override
	public void readFields(DataInput in) throws IOException {
		wid = in.readLong();
		text = Text.readString(in);
	}

	@Override
	public void readFields(ResultSet rs) throws SQLException {
		wid = rs.getLong(1);
		text = rs.getString(2);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(wid);
		Text.writeString(out, text);
	}

	@Override
	public void write(PreparedStatement ps) throws SQLException {
		ps.setLong(1, wid);
		ps.setString(2, text);
	}

	public long getWid() {
		return wid;
	}

	public String getText() {
		return text;
	}

}
