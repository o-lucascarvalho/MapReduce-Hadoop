package basic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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


public class TransactionsEx1 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("output/TransactionsEx1.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "TransactionEx1");

        // registro das classes
        j.setJarByClass(TransactionsEx1.class);
        j.setMapperClass(MapForTransactionsEx1.class);
        j.setReducerClass(ReduceForTransactionsEx1.class);

        // definicao dos tipos de saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

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
    public static class MapForTransactionsEx1 extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // Carregamento do bloco e conversao dele para string e na sequencia quebrando a cada ";".
            String[] line = value.toString().split(";");

            // Pegando a coluna 0 que refere-se ao Pais
            String pais = line[0];

            // Criando Chave que é o Pais
            Text outputKey = new Text(pais);
            // Criando valor que é o numero 1 para cada ocorrência.
            IntWritable outputValue = new IntWritable(1);
            con.write(outputKey, outputValue);
        }
    }


    /**
     * Classe de Reduce
     * Formato geral: Reducer<Tipo da chave de Entrada,
     *                        Tipo do Valor de Entrada,
     *                        Tipo da chave de Saida,
     *                        Tipo do Valor de Saida>
     *
     *
     * Importante: note que o tipo do valor de entrada n eh uma lista!
     */
    public static class ReduceForTransactionsEx1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Funcao de reduce
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            // Efetuando a soma de ocorrência para cada País.
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            // Saída no formato <País, somatorio de ocorrencias>
            con.write(key, new IntWritable(sum));
        }
    }

}
