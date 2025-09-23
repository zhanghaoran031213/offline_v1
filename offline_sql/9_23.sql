select
    ifnull(
            (
                select distinct salary
                from employee
                order by salary desc
                limit 1 offset 1
        ), null) as secondhighestsalary;


select
    ifnull(round(count(distinct session_id) / count(distinct user_id),2),0) as average_sessions_per_user
from Activity
where datediff('2019-07-27',activity_date) < 30;


select *
from (select class,count(distinct student) as num
      from courses
      group by class) as temp_table
where num >= 5;



select
    round(
            ifnull(
                        (select count(*) from (select distinct requester_id,accepter_id from RequestAccepted) as A)
                        /
                        (select count(*) from (select distinct sender_id,send_to_id from FriendRequest) as B)
                ,0)
        ,2) as accept_rate;




select
    max(num) as num
from (
         select num
         from my_numbers
         group by num
         having count(num) = 1
     ) as t;