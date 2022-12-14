## Pyspark_Practice

### Q1. Write a script for the below scenario either in PySpark (or) Spark Scala, refer assignment.txt

#### 1. Read the input testfile (Pipe delimited) provided as a "Spark RDD" 

#### 2. Remove the Header Record from the RDD

#### 3. Calculate Final_Price:

    Final_Price = (Size * Price_SQ_FT)
    
#### 4. Save the final rdd asTextfile with three fields

     Property_ID|Location|Final_Price
     

### Q2. Write a script for the below scenario either in PySpark (or) Spark Scala, refer assignment.txt

#### 1. Read the input testfile (Pipe delimited) provided as a "Spark RDD" 

#### 2. Remove the Header Record from the RDD

#### 3. Add a new column as entry_code which would be a combination of last 3 digits of property_id and first 2 digits of Location.

#### 4. Save the final as Textfile with two fields

      entry_code|Status
      

### Q3. We need to find out the top 2 people who has the highest weight(in kgs).

#### Input:
           
             Name|Weight
             Roy|55
             Mary|60
             Tom|65
             Ashish|70
             Deepak|85

#### Output:
 
             Deepak
             Ashish
             
             
 ### Q4. We need to find out the people whose weight is greater than 60(in kgs).
 
 #### Input:
           
             Name|Weight
             Roy|55
             Mary|60
             Tom|65
             Ashish|70
             Deepak|85
             
 #### Output:
 
             Deepak
             Ashish
             Tom
 
### Q5. We need to find out the person with the second highest weight.

#### Input:
           
             Name|Weight
             Roy|55
             Mary|60
             Tom|65
             Ashish|70
             Deepak|85
             
 #### Output:
          
          Ashish


### Q6. Given the csv file has 3 columns Name, Age and hobbies are seperated by comma(,)

#### 1. Hobbies field can have 1 or more values separated by hyphen(-)

#### 2.Each employee can have only 1 record in this file

#### 3. Read the csv file using Spark API and convert each row into mutiple rows based on hobbies


#### Input:      
            Name, Age,hobbies
            Chandrali,28,singing-dancing-reading


#### Output

            Name, Age,hobbies
            Chandrali,28,singing
            Chandrali,28,dancing
            Chandrali,28,reading
