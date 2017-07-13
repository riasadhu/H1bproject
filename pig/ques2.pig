2a) Which part of the US has the most Data Engineer jobs for each year?
   b) find top 5 locations in the US who have got certified visa for each year.




2a)bag1 = load '/user/hive/warehouse/h1bapplication.db/h1b_final' using PigStorage() as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:int,year:int,worksite:chararray,long:double,lati:double);
bag2 = filter bag1 by ($4 MATCHES '.*DATA ENGINEER.*');
bag3 = group bag2 by (year,worksite);
bag4 = foreach bag3 generate group,COUNT(bag2) as total;
bag5 = foreach bag4 generate FLATTEN(group),total;
bag6 = group bag5 by year;
top = foreach bag6 {
sorted = order bag5 by total desc;
top1 = limit sorted 1;
generate flatten(top1);
};
dump top;


2 b)bag1 = load '/user/hive/warehouse/h1bapplication.db/h1b_final' using PigStorage() as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:int,year:int,worksite:chararray,long:double,lati:double);
bag2 = filter bag1 by ($1 MATCHES 'CERTIFIED');
bag3 = group bag2 by (year,worksite);
bag4 = foreach bag3 generate group,COUNT(bag2) as total;
bag5 = foreach bag4 generate FLATTEN(group),total;
bag6 = group bag5 by year;
bag7 = foreach bag6
{
sorted = order bag5 by total desc;
top5 = limit sorted 5;
generate FLATTEN(top5);
}
dump bag7;

 
