package org.wuruihe.key;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class DateDimension extends BaseDimension {
    private String year;
    private String month;
    private String day;

    //无参数构造啊
    public DateDimension() {

    }

    public DateDimension(String year, String month, String day) {
        this.year = year;
        this.month = month;
        this.day = day;
    }

    public int compareTo(BaseDimension o) {
        DateDimension other = (DateDimension) o;
        //先判断是否相同
        int result = this.year.compareTo(other.year);
        if (result == 0) {
            //判断月是否相同
            result = this.month.compareTo(other.month);
        }
        if (result == 0) {
            result = this.day.compareTo(other.day);

        }
        return result;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(year);
        dataOutput.writeUTF(month);
        dataOutput.writeUTF(day);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readUTF();
        this.month = dataInput.readUTF();
        this.day = dataInput.readUTF();
    }
    //get

    public String getYear() {
        return year;
    }

    public String getMonth() {
        return month;
    }

    public String getDay() {
        return day;
    }

    //set

    public void setYear(String year) {
        this.year = year;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public void setDay(String day) {
        this.day = day;
    }

    //toString

    @Override
    public String toString() {
        return year + "\t" + month + "\t" + day;
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DateDimension that = (DateDimension) o;

        if (year != null ? !year.equals(that.year) : that.year != null) return false;
        if (month != null ? !month.equals(that.month) : that.month != null) return false;
        return day != null ? day.equals(that.day) : that.day == null;
    }

    @Override
    public int hashCode() {
        int result = year != null ? year.hashCode() : 0;
        result = 31 * result + (month != null ? month.hashCode() : 0);
        result = 31 * result + (day != null ? day.hashCode() : 0);
        return result;
    }
}
