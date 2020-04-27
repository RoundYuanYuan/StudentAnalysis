package com.hiveclient.core;


import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.SparkConf;

public class GenerateModel {
    public void generateModel(){
        System.setProperty("user.name", "root");
        SparkConf sparkConf = new SparkConf().setAppName("JavaNaiveBayesExample12").setMaster("local");//spark://192.168.88.21:7077
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        String path = "hdfs://192.168.88.21:9000/user/hive/warehouse/finaldesign.db/t_trans/t_student.csv";
        JavaRDD<String> lines = jsc.textFile(path);
        JavaRDD<LabeledPoint> parsedData = lines.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String s) {
                String[] source = s.split(",");
                Double dLabel = Double.valueOf(source[13]);
                String[] sFeatures = new String[13];
                System.arraycopy(source, 0, sFeatures, 0, 13);
                double[] values = new double[sFeatures.length];
                for (int i = 0; i < sFeatures.length; i++) {
                    values[i] = Double.parseDouble(sFeatures[i]);
                }
                LabeledPoint lp = new LabeledPoint(dLabel, Vectors.dense(values));//Vectors本地向量密度向量
                return lp;
            }
        });
        parsedData.cache();
        JavaRDD<LabeledPoint>[] tmp = parsedData.randomSplit(new double[]{0.7, 0.3});
        JavaRDD<LabeledPoint> training = tmp[0]; // training set
        JavaRDD<LabeledPoint> test = tmp[1]; // test set
        NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);
        JavaPairRDD<Double, Double> predictionAndLabel =
                test.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
        double accuracy =
                predictionAndLabel.filter(pl -> pl._1().equals(pl._2())).count() / (double) test.count();
        System.out.println(accuracy);
        // Save and load model
        model.save(jsc.sc(), "hdfs://192.168.88.21:9000/model/salary");
        //NaiveBayesModel sameModel = NaiveBayesModel.load(jsc.sc(), "hdfs://192.168.88.21:9000/model/salary");
        jsc.stop();
    }
}

