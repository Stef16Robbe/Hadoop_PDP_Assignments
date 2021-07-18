import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("Linear Regression Model").config("spark.executor.memory", "1gb").getOrCreate()

sc = spark.sparkContext

df = spark.read.format("CSV").option("header", "true").load(".\\titanic.csv")
df = df.withColumn("Survived", df["Survived"].cast(IntegerType())) \
    .withColumn("Pclass", df["Pclass"].cast(IntegerType())) \
    .withColumn("Name", df["Name"].cast(StringType())) \
    .withColumn("Sex", df["Sex"].cast(StringType())) \
    .withColumn("Age", df["Age"].cast(IntegerType())) \
    .withColumn("Siblings/Spouses Aboard", df["Siblings/Spouses Aboard"].cast(IntegerType())) \
    .withColumn("Parents/Children Aboard", df["Parents/Children Aboard"].cast(IntegerType())) \
    .withColumn("Fare", df["Fare"].cast(FloatType()))

# A:
dfA = df.select("Survived", "Pclass", "Sex")
dfA = dfA.groupBy("Sex", "Pclass").avg("Survived")
dfA.show()

# B:
dfB = df.select("Age", "Pclass", "Survived").toPandas()
dfB = dfB[(dfB['Age'] <= 10) & (dfB['Pclass'] == 3)][['Age', 'Survived', 'Pclass']]
survived_y = dfB[dfB['Survived'] == 1].count()[0]
prob = (survived_y / dfB.count()[0] * 100).item()
print("probability to survive: " + str(prob))

# C
dfC = df.select("Pclass", "Fare")
dfC = dfC.groupBy("Pclass").avg("Fare")
dfC.show()
