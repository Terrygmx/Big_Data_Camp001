## 操作步骤

## 1. 登录hive
登陆上服务器，输入hive，进入交互界面。

## 2. 建立数据表
### movie表

```sql
create external table terry_movie(    
    MovieID      int
   ,MovieName    string
   ,MovieType   array<string>
)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ("field.delim"="::","collection.delim"="|")
location '/data/hive/movies';
```

### user表
```sql
create external table terry_user(    
    UserID      int
   ,Sex    string
   ,Age      int
   ,Occupation      int
   ,Zipcode    string
)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ("field.delim"="::")
location '/data/hive/users';
```

### rating表

```sql
create external table terry_ratings(    

    UserID      int
   ,MovieID    int
   ,Rate      int
   ,Times      int
)
 ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ("field.delim"="::")
location '/data/hive/ratings';
```

## 作业1
题目一（简单）
展示电影 ID 为 2116 这部电影各年龄段的平均影评分。

### 解答：
```sql
select u.age age, avg(r.rate) avgrate  from terry_user as u inner 
join terry_ratings as r 
on u.userid=r.userid and r.movieId=2116 
group by u.age;
```
## 题目二（中等）
找出男性评分最高且评分次数超过 50 次的 10 部电影，展示电影名，平均影评分和评分次数。

筛选评分次数超过 50 次的 10 部电影：
```sql
select movieid, avg(rate) avgrate, count(*) times
from terry_ratings
group by movieid
having times>50
order by avgrate desc
limit 10;
```

筛选男性用户的子查询：
```sql
select u.sex sex, r.movieid movieid, avg(r.rate) avgrate ,count(*) times
from terry_user as u inner join terry_ratings as r
on u.userid = r.userid  and  sex = 'M'
group by u.sex,r.movieid
having times>50
order by avgrate desc
limit 10
;
```
### 解答：
```sql
select ur.sex sex, m.movieName name, ur.avgrate avgrate, ur.times total
from terry_movie as m inner join 
(
    select u.sex sex, r.movieid movieid, avg(r.rate) avgrate ,count(*) times
from terry_user as u inner join terry_ratings as r
on u.userid = r.userid  and  sex = 'M'
group by u.sex,r.movieid
having times>50
order by avgrate desc
limit 10
) as ur 
on m.movieid=ur.movieid 
limit 10
;
```


删除表

 drop table terry_movie;
 drop table terry_ratings;
 drop table terry_user;

 ## 第三题
 题目三（选做）
找出影评次数最多的女士所给出最高分的 10 部电影的平均影评分，展示电影名和平均影评分（可使用多行 SQL）。

找出影评次数最多的女士
<!-- ```sql
select u.userid,u.sex, count(*) times  from terry_user as u 
inner join terry_ratings as r 
on u.userid=r.userid and u.sex='F' 
group by u.userid,u.sex
order by times desc
limit 1;
``` -->
```sql
CREATE VIEW mostRateFrau(userid COMMENT 'userid of the Frau',
    sex,times
)
  AS
  select u.userid,u.sex, count(*) times  from terry_user as u 
    inner join terry_ratings as r 
    on u.userid=r.userid and u.sex='F' 
    group by u.userid,u.sex
    order by times desc
    limit 1;

```



得到该用户userid为1150, 有1302部电影评分。

由她所给出最高分的 10 部电影
```sql
select r.movieid  from terry_ratings as r
inner join mostRateFrau as m
where r.userid=m.userid
order by rate desc
limit 10;
```

展示此10部电影的平均得分
```sql
select r.movieid, avg(r.rate) avgrate from terry_ratings as r
inner join (
    select r.movieid  from terry_ratings as r
    inner join mostRateFrau as m
    where r.userid=m.userid
    order by r.rate desc
    limit 10
) as m
on r.movieid = m.movieid
group by r.movieid
limit 10;
```
### 解答：
第一步，建立view，找出影评次数最多的女士：
```sql
CREATE VIEW mostRateFrau(userid COMMENT 'userid of the Frau',
    sex,times
)
  AS
  select u.userid,u.sex, count(*) times  from terry_user as u 
    inner join terry_ratings as r 
    on u.userid=r.userid and u.sex='F' 
    group by u.userid,u.sex
    order by times desc
    limit 1;

```
第二步，使用viwe进行join查询：
```sql
-- 最外层用于获取电影名称
select mr.movieName, r1.avgrate avgrate from terry_movie as mr
inner join (
    -- 展示此10部电影的平均得分
        select r2.movieid, avg(r2.rate) avgrate from terry_ratings as r2
    inner join (
        -- 找出影评次数最多的女士给出最高分的 10 部电影
        select r.movieid,r.rate  from terry_ratings as r
        inner join mostRateFrau as m
        where r.userid=m.userid
        order by r.rate desc
        limit 10
    ) as m
    on r2.movieid = m.movieid
    group by r2.movieid
    limit 10
) as r1
on r1.movieid = mr.movieid
limit 10;
```


