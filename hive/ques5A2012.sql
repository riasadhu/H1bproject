select job_title,year,count(job_title) as count from h1bapplication.5year11 where year=2012 group by job_title,year order by count desc limit 10;

