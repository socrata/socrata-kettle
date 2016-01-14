# Date Difference Calculator

This transform is meant to help take a Socrata integer filter and let you use a date within it to build dynamic date filters.  Socrata Date Column filters work off of a specific date.  You could for example, use this transform to build a map of crimes within the last seven days.  There are two steps to the transform.  
## Step 1
Get a System date. In other words what day is it the day the transform runs. 
   
## Step 2
Calculate the difference between day the transform is ran and the date column in the underlying data.  This outputs as an integer.  

##Results 
A dynamic date filter by adding a date differnce number column to your underlying dataset.  
