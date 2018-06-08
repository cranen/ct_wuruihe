package org.wuruihe.key;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CommDimension extends BaseDimension {

    private ContactDimension contactDimension=new ContactDimension();
    private DateDimension dateDimension=new DateDimension();

    public CommDimension() { }

    /**
     * 此为对key的定义
     * @param o
     * @return
     */
    public int compareTo(BaseDimension o) {

        CommDimension other = (CommDimension) o;
        int result = this.contactDimension.compareTo(other.contactDimension);
        if(result==0){
            result=this.dateDimension.compareTo(other.dateDimension);
        }
        return result;

    }



    public void write(DataOutput dataOutput) throws IOException {
        //调用的子类的实用的方法
        this.contactDimension.write(dataOutput);
        this.dateDimension.write(dataOutput);
       // dataOutput.
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.contactDimension.readFields(dataInput);
        this.dateDimension.readFields(dataInput);

    }

    public ContactDimension getContactDimension() {
        return contactDimension;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setContactDimension(ContactDimension contactDimension) {
        this.contactDimension = contactDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }



}
