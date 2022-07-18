
import numpy as np
 



# COMMAND ----------

# Creating arrays
arr = np.array([10,20,30]) 
print (arr)

 


# COMMAND ----------

#ChecKType
BigDataNumpy = np.array([80,200,30,899,99])  
print("The Item Type is:", BigDataNumpy.dtype)

# COMMAND ----------

print(BigDataNumpy[2])

# COMMAND ----------

#Describe the  structure of row,col
BigDataShape = np.array([[9,193,8],[6,10,6]]) 
print(BigDataShape.shape)

# COMMAND ----------

#Take Square
Myarray1= np.array([2,4,6,8,10])
Myarray = np.square(Myarray1)
print(Myarray)

# COMMAND ----------

# Create Series from array
import pandas as pd 
import numpy as np
MySubject = np.array(['BigData','DataScience','DeepLearning'])
Inseries = pd.Series(MySubject)
print (series)

# COMMAND ----------

#Createing Rdd from different ways and performing action

# COMMAND ----------

ParrRdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

# COMMAND ----------

BigData = sc.textFile("dbfs:/FileStore/shared_uploads/ahmedhassan0057@gmail.com/BigDataRdd.txt")

# COMMAND ----------

ParrRdd.take(3)


# COMMAND ----------

BigData.take(2)

# COMMAND ----------

  print("countByValue :  "+ParrRdd.countByValue())

# COMMAND ----------

ParrRdd.collect()
#collect data

# COMMAND ----------

BigData.first()
#take first value

# COMMAND ----------


print(ParrRdd.reduce(lambda x, y : x + y))
#Add All

# COMMAND ----------

print(ParrRdd.filter(lambda x: x%4 == 0).collect())
#filter values divide by 4

# COMMAND ----------

print(BigData.filter(lambda x: x.startswith('D')).collect())
#filter data start with D

# COMMAND ----------

SplitRdd= sc.parallelize([ "This is a Big Data Project"])
(SplitRdd.flatMap(lambda x: x.split(" ")).collect())
    