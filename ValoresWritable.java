package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class ValoresWritable implements WritableComparable<ValoresWritable> {

    private float pm;
    private float valor;


    public ValoresWritable() {
    }

    public ValoresWritable(float qtd, float valor) {
        this.pm = qtd;
        this.valor = valor;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        valor = Float.parseFloat(in.readUTF());
        pm = Float.parseFloat(in.readUTF());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(valor));
        out.writeUTF(String.valueOf(pm));
    }


    public float getpm() {
        return pm;
    }

    public float getvalor() {
        return valor;
    }

    public void setpm(float qtd) {
        this.pm = qtd;
    }

    public void setvalot(float valor) {
        this.valor = valor;
    }

    public String toString() {
        return "(" + valor + "," + pm + ")";
    }

    @Override
    public int compareTo(ValoresWritable o) {
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
        ValoresWritable that = (ValoresWritable) o;
        return Float.compare(that.pm, pm) == 0 && Float.compare(that.valor, valor) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pm, valor);
    }


}
