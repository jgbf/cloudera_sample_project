from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler


def main()
    # Read source data
    data = spark.read.option('header', 'true').csv('/nifi/decompressed/')

    # Convert columns to integer
    data = data.withColumn('month', col('Month').cast('int')) \
        .withColumn('day_of_week', col('DayOfWeek').cast('int')) \
        .withColumn('air_time', col('AirTime').cast('int')) \
        .withColumn('dep_time', col('DepTime').cast('int')) \
        .withColumn('crs_app_time', col('CRSArrTime').cast('int')) \
        .withColumn('dist', col('Distance').cast('int')) \
        .withColumn('air_time', col('AirTime').cast('int')) \
        .withColumn('year', col('Year').cast('int')) \
        .withColumn('del1', col('ArrDelay').cast('int')) \
        .withColumn('del2', col('DepDelay').cast('int')) \
        .withColumn('del3', col('CarrierDelay').cast('int')) \
        .withColumn('del4', col('WeatherDelay').cast('int')) \
        .withColumn('del5', col('NASDelay').cast('int')) \
        .withColumn('del6', col('SecurityDelay').cast('int')) \
        .withColumn('del7', col('LateAircraftDelay').cast('int'))

    # Add total delay column
    data = data.withColumn(
        'tot_delay', 
        col('del1') 
        + col('del2') 
        + col('del3') 
        + col('del4') 
        + col('del5') 
        + col('del6') 
        + col('del7')
    )

    # Calculate Correlations
    for column in target_cols:
        print(f'The correlation to {column} is {data.stat.corr(column, "tot_delay")}')

    # Create data vectors for train data
    target_cols = ['day_of_week', 'air_time', 'dep_time', 'crs_app_time', 'dist']
    data = data.na.fill(0)
    vectorAssembler = VectorAssembler(inputCols = target_cols, outputCol = 'features')
    vectorised = vectorAssembler.transform(data)
    vectorised = vectorised.select(['features', 'tot_delay'])

    # Split data to train and test 
    splits = vectorised.randomSplit([0.7, 0.3])
    train_df = splits[0]
    test_df = splits[1]

    # Train linear regression model
    lr = LinearRegression(
        featuresCol='features', 
        labelCol='tot_delay', 
        maxIter=10, 
        regParam=0.3, 
        elasticNetParam=0.8
    )

    lr_model = lr.fit(train_df)

    # Print model statistics
    print("Coefficients: " + str(lr_model.coefficients))
    print("Intercept: " + str(lr_model.intercept))

    trainingSummary = lr_model.summary
    print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
    print("r2: %f" % trainingSummary.r2)


    # Evaluate model
    lr_evaluator = RegressionEvaluator(
        predictionCol="prediction", 
        labelCol="tot_delay",
        metricName="r2"
    )

    test_result = lr_model.evaluate(test_df)
    print("Root Mean Squared Error (RMSE) on test data = %g" % test_result.rootMeanSquaredError)


    # Predict on test data
    predictions = lr_model.transform(test_df)
    predictions.select("prediction", "tot_delay", "features").show()

if __name__ == '__main__':
    main()
