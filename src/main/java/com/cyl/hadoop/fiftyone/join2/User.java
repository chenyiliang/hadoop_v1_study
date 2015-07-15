package com.cyl.hadoop.fiftyone.join2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class User implements WritableComparable<User> {
	private String userNo = "";
	private String userName = "";
	private String cityName = "";
	private String cityNo = "";
	private int flag;

	public User() {
	};

	public User(User u) {
		this.userNo = u.getUserNo();
		this.userName = u.getUserName();
		this.cityNo = u.getCityNo();
		this.cityName = u.getCityName();
		this.flag = u.getFlag();
	}

	public User(String userNo, String userName, String cityNo, String cityName,
			int flag) {
		this.userName = userName;
		this.userNo = userNo;
		this.cityName = cityName;
		this.cityNo = cityNo;
		this.flag = flag;
	}

	public String getUserNo() {
		return userNo;
	}

	public void setUserNo(String userNo) {
		this.userNo = userNo;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getCityName() {
		return cityName;
	}

	public void setCityName(String cityName) {
		this.cityName = cityName;
	}

	public String getCityNo() {
		return cityNo;
	}

	public void setCityNo(String cityNo) {
		this.cityNo = cityNo;
	}

	public int getFlag() {
		return flag;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(this.userNo);
		output.writeUTF(this.userName);
		output.writeUTF(this.cityName);
		output.writeUTF(this.cityNo);
		output.writeInt(this.flag);
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		this.userNo = input.readUTF();
		this.userName = input.readUTF();
		this.cityName = input.readUTF();
		this.cityNo = input.readUTF();
		this.flag = input.readInt();
	}

	@Override
	public int compareTo(User o) {
		return 0;
	}

	@Override
	public String toString() {
		String string = this.userNo + "," + this.userName + "," + this.cityName;
		return string;
	}

}
