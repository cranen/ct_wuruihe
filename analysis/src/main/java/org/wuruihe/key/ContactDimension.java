package org.wuruihe.key;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 联系人维度
 */
public class ContactDimension extends BaseDimension {
    //姓名
    private String name ;
    //电话号码
    private String phoneNum;

    public ContactDimension() { }


    @Override
    public String toString() {
        return  name +"\t"+ phoneNum ;
    }

    public String getName() {
        return name;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }



    public int compareTo(BaseDimension o) {
        //向下转型
        ContactDimension other=(ContactDimension)o;
        return phoneNum.compareTo(other.phoneNum);
    }


    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(phoneNum);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.name=dataInput.readUTF();
        this.phoneNum=dataInput.readUTF();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ContactDimension that = (ContactDimension) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return phoneNum != null ? phoneNum.equals(that.phoneNum) : that.phoneNum == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (phoneNum != null ? phoneNum.hashCode() : 0);
        return result;
    }
}
