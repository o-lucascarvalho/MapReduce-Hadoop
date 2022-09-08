package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class TransactionsEx3 {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("output/TransactionsEx3.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "TransactionsEx3");

        // registro das classes
        j.setJarByClass(TransactionsEx3.class);
        j.setMapperClass(MapForTransactionsEx3.class);
        j.setReducerClass(ReduceForTransactionsEx3.class);

        // definicao dos tipos de saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(ValoresWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);

        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    /**
     * Classe de MAP
     * Formato geral: Mapper<Tipo da chave de entrada,
     *                       Tipo da entrada,
     *                       Tipo da chave de saida,
     *                       Tipo da saida>
     */
    public static class MapForTransactionsEx3 extends Mapper<LongWritable, Text, Text, ValoresWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            try {
                // Carregamento do bloco e conversao dele para string
                String line = value.toString();

                // fazendo o split para obter as palavras de forma isolada e armazenando Ano e Fluxo em Variáveis
                String [] values = line.split(";");
                String ano = values[1];
                String fluxo = values[4];

                // Validando se a coluna com valor, dividido pela quantidade é um valor válido
                // Desta forma ignoramos a informação do header.
                // Passando informação de preço médio e ocorrencia
                float Preco = Float.parseFloat(values[5]) / Float.parseFloat(values[8]);
                if (Preco > 0) {
                    // Chave
                    Text outputKey = new Text(ano + " - " + fluxo);
                    // Valor
                    ValoresWritable val = new ValoresWritable(Preco, Float.parseFloat("1.0"));
                    con.write(outputKey, val);
                }
            }
            catch(Exception ignored) { }

        }
    }
    /**
     * Classe de Reduce
     * Formato geral: Reducer<Tipo da chave de Entrada,
     *                        Tipo do Valor de Entrada,
     *                        Tipo da chave de Saida,
     *                        Tipo do Valor de Saida>
     * Importante: note que o tipo do valor de entrada n eh uma lista!
     */
    public static class ReduceForTransactionsEx3 extends Reducer<Text, ValoresWritable, Text, FloatWritable> {

        // Funcao de reduce
        public void reduce(Text key, Iterable<ValoresWritable> values, Context con) throws IOException, InterruptedException {
            float sumvlr = 0;
            float sumpm = 0;
            for (ValoresWritable val : values) {
                sumpm += val.getpm();
                sumvlr += val.getvalor();
            }
            con.write(key, new FloatWritable(sumpm/sumvlr));

        }
    }
}
