I have a better idea on how i want the architecture to go now

keep the district partitioning, but for the processing layer i will have three layers

a snapshot layer which is the first one that produces static statistics, it will run on the data available for that district up until today 
a second layer to do the cross-partition analysis where you roll up insights across districts
and the third layer will be a time series/historical tracking layer


any stats that i want to embed in this needs to sit in one or the other

the second layer will basically just roll up all of the static insights produced over the days to see if there are trends up until there 

the next thing is that i will not be bottlenecked by having to wait for time to pass to test the processing, instead i will create a script that will take the full data set then it will use the created date to create faux ingestion paths with synthetic data (all data up until that date, basically simulating what the system would have done if i had built it days before)




---
rename to metrics then bring api code under here too, keeping as dataaggregator in case this is where i write to events api as well