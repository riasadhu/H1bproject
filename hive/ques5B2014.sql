select jobtile,year,count(jobtile),case_status as count from h1bapplication.5byear11 where year=2014 and case_status =='CERTIFIED' group by jobtile,year,case_status order by count desc limit 10;
