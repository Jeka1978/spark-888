package linkedIn;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.*;

/**
 * Created by Evegeny on 28/12/2016.
 */
@Service
public class LinkedInService {
    private static final String SALARY = "salary";
    private static final String KEYWORDS = "keywords";
    private static final String KEYWORD = "keyword";
    public static final String AMOUNT = "amount";
    private static final String NAME = "name";
    @Autowired
    private SQLContext sqlContext;

    public void printAll() {
        DataFrame dataFrame = sqlContext.read()
                .json("data/linkedIn/*.json");
        dataFrame.show();
        dataFrame.printSchema();
        StructField[] fields = dataFrame.schema().fields();
        for (StructField field : fields) {
            System.out.println(field.name());
            System.out.println(field.dataType());
        }

        dataFrame = dataFrame.withColumn(SALARY,
                col("age").multiply(10)
                        .multiply(size(col(KEYWORDS))));
        dataFrame.show();
        Row row = dataFrame.withColumn(KEYWORD, explode(col(KEYWORDS))).select(KEYWORD)
                .groupBy(KEYWORD).agg(count("keyword").as("amount"))
                .orderBy(col(AMOUNT).desc()).head();
        String mostPopular = row.getString(0);
        Row[] rows = dataFrame.filter(col(SALARY).leq(2500).
                and(array_contains(col(KEYWORDS), mostPopular))).collect();
        int fieldIndex = dataFrame.schema().fieldIndex(NAME);
        for (Row row1 : rows) {
            System.out.println(row1.get(fieldIndex));
        }
        while (true) {
            
        }
    }
}










