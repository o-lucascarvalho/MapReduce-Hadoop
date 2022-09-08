package basic;

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


public class TransactionsEx5 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // arquivo de saida
        Path output = new Path("output/TransactionsEx5.txt");

        Path output2 = new Path("output/TransactionsEx5Final.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "TransactionsEx5");

        // registro das classes
        j.setJarByClass(TransactionsEx5.class);
        j.setMapperClass(MapForTransactionsEx5.class);
        j.setReducerClass(ReduceForTransactionsEx5.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FloatWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);


        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);


        Job j2 = new Job(c, "TransactionsEx5Final");

        // registro das classes
        j2.setJarByClass(TransactionsEx5.class);
        j2.setMapperClass(MapForTransactionsEx5Final.class);
        j2.setReducerClass(ReduceForTransactionsEx5Final.class);

        // definicao dos tipos de saida
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(commodityQtd.class);

        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);


        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j2, output);
        FileOutputFormat.setOutputPath(j2, output2);

        // lanca o job e aguarda sua execucao
        j.waitForCompletion(true);
        j2.waitForCompletion(true);
    }

    /**
     * Classe de MAP
     * Formato geral: Mapper<Tipo da chave de entrada,
     *                       Tipo da entrada,
     *                       Tipo da chave de saida,
     *                       Tipo da saida>
     */
    public static class MapForTransactionsEx5 extends Mapper<LongWritable, Text, Text, FloatWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // Carregamento do bloco e conversao dele para string
            String line = value.toString();

            // fazendo o split para obter as palavras de forma isolada
            String[] colunas = line.split(";");

            try {
                if(colunas[1].equals("2016")) {

                    //float Preco = Float.parseFloat(colunas[5]) / Float.parseFloat(colunas[8]);
                    float qtd = Float.parseFloat(colunas[8]);

                    // Chave
                    Text outputKey = new Text(colunas[4] + "-&&-" + colunas[3]);
                    con.write(outputKey, new FloatWritable(qtd));
                }

            } catch (Exception ignored) {


            }

        }
    }
    public static class MapForTransactionsEx5Final extends Mapper<LongWritable, Text, Text, commodityQtd> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // Carregamento do bloco e conversao dele para string
            String line = value.toString();

            // fazendo o split para obter as palavras de forma isolada
            String[] colunas = line.split("-&&-");
            String[] colunas2 = colunas[1].split("\t");


            try {

                // Chave
                Text outputKey = new Text(colunas[0]);
                // Valor
                float qtd = Float.parseFloat(colunas2[1]);
                String commodity_name = colunas2[0];

                commodityQtd outputValue = new commodityQtd(commodity_name, qtd);

                con.write(outputKey, outputValue);


            } catch (Exception ignored) {


            }

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
    public static class ReduceForTransactionsEx5 extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        // Funcao de reduce
        public void reduce(Text key, Iterable<FloatWritable> values, Context con)
                throws IOException, InterruptedException {
            // Objetivo: somar as ocorrencias de uma palavra
            // Note que o reduce vai ser chamado uma vez por chave,
            // entao nao precisamos filtrar por palavra
            float sum = 0;
            for (FloatWritable value : values) {
                sum += value.get();
            }
            // faz a saida no formato <palavra, somatorio de ocorrencias>
            con.write(key, new FloatWritable(sum));
        }
    }

    public static class ReduceForTransactionsEx5Final extends Reducer<Text, commodityQtd, Text, Text> {

        // Funcao de reduce
        public void reduce(Text key, Iterable<commodityQtd> values, Context con)
                throws IOException, InterruptedException {
            // Objetivo: somar as ocorrencias de uma palavra
            // Note que o reduce vai ser chamado uma vez por chave,
            // entao nao precisamos filtrar por palavra
            String commodityMax = "";
            Float compareQtd = 0.0f;
            for (commodityQtd value : values) {

                if (value.getQtd()> compareQtd) {
                    commodityMax = value.getCommodity();
                    compareQtd = value.getQtd();

                }
            }
            Text MaxComm = new Text(commodityMax);
            con.write(key, MaxComm);

        }


    }


}
