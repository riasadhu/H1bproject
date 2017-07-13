select soc_name ,count(soc_name) as soc_count from h1bapplication.h1b_final where job_title like'%DATA SCIENTIST%' group by soc_name order by soc_count desc limit 1;

