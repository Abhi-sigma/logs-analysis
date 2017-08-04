 create or replace view popular_article as
 select articles.title as title,authors.name as name,
 count(articles.title) as views from articles,log,authors
 where articles.author=authors.id and log.path = '/article/' || articles.slug 
 group by articles.title,authors.name order by views desc;


 create or replace view requests as
 select all_request.datet,all_request.total_request,
 request_fails.failures from (select (date(log.time))
 as datet,count(date(log.time)) as total_request
 from log group by date(log.time)) as all_request
 join
 (select date(log.time) as datef,count(log.status)
  as failures from log where log.status !='200 OK'
  group by date(log.time),log.status) as request_fails
  on all_request.datet = request_fails.datef order by all_request.datet;