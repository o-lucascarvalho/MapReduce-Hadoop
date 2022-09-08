package basic;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class commodityQtd implements WritableComparable<commodityQtd> {

    private String commodity;
    private float qtd;

    public commodityQtd() {
    }

    public commodityQtd(String commodity, Float qtd) {
        this.qtd = qtd;
        this.commodity = commodity;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        commodity = in.readUTF();
        qtd = Float.parseFloat(in.readUTF());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(commodity));
        out.writeUTF(String.valueOf(qtd));
    }

    public Float getQtd() {
        return qtd;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setQtd(float qtd) {
        this.qtd = qtd;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public String toString() {
        return "(" + commodity + "," + qtd + ")";
    }

    @Override
    public int compareTo(commodityQtd o) {
        if(this.hashCode() < o.hashCode()){
            return -1;
        }else if(this.hashCode() > o.hashCode()){
            return +1;
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        commodityQtd that = (commodityQtd) o;
        return Float.compare(that.qtd, qtd) == 0 && Objects.equals(commodity, that.commodity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, qtd);
    }
}
