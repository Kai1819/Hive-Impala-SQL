第一题
我们有如下的用户访问数据
userId  visitDate   visitCount
    u01 2017/1/21   5
    u02 2017/1/23   6
    u03 2017/1/22   8
    u04 2017/1/20   3
    u01 2017/1/23   6
    u01 2017/2/21   8
    U02 2017/1/23   6
    U01 2017/2/22   4
要求使用SQL统计出每个用户的累积访问次数，如下表所示：
用户id    月份  小计  累积
    u01 2017-01 11  11
    u01 2017-02 12  23
    u02 2017-01 12  12
    u03 2017-01 8   8
    u04 2017-01 3   3
实现

CREATE TABLE test_sql.test1 ( 
        userId string, 
        visitDate string,
        visitCount INT )
    ROW format delimited FIELDS TERMINATED BY "\t";
INSERT INTO TABLE test_sql.test1
VALUES
    ( 'u01', '2017/1/21', 5 ),
    ( 'u02', '2017/1/23', 6 ),
    ( 'u03', '2017/1/22', 8 ),
    ( 'u04', '2017/1/20', 3 ),
    ( 'u01', '2017/1/23', 6 ),
    ( 'u01', '2017/2/21', 8 ),
    ( 'u02', '2017/1/23', 6 ),
    ( 'u01', '2017/2/22', 4 );


select *,date_format(regexp_replace(visitDate,'/','-'),'yyyy-MM') from test_sql.test1

我的SQL
select t.userId,t.mon,t.sub_tal,sum(t.sub_tal) over(partition by t.userId order by mon) cum_tal
from
(
select userId,date_format(regexp_replace(visitDate,'/','-'),'yyyy-MM') mon,sum(visitCount) sub_tal
from test_sql.test1
group by userId,date_format(regexp_replace(visitDate,'/','-'),'yyyy-MM')
) t
	
官方SQL
SELECT t2.userid,
       t2.visitmonth,
       subtotal_visit_cnt,
       sum(subtotal_visit_cnt) over (partition BY userid ORDER BY visitmonth) AS total_visit_cnt
FROM
  (SELECT userid,
          visitmonth,
          sum(visitcount) AS subtotal_visit_cnt
   FROM
     (SELECT userid,
             date_format(regexp_replace(visitdate,'/','-'),'yyyy-MM') AS visitmonth,
             visitcount
      FROM test_sql.test1) t1
   GROUP BY userid,
            visitmonth)t2
ORDER BY t2.userid,
         t2.visitmonth

----------------------------------------------------------------------------------------------------------------------------------

第二题
有50W个京东店铺，每个顾客访客访问任何一个店铺的任何一个商品时都会产生一条访问日志，
访问日志存储的表名为Visit，访客的用户id为user_id，被访问的店铺名称为shop，数据如下：

                u1  a
                u2  b
                u1  b
                u1  a
                u3  c
                u4  b
                u1  a
                u2  c
                u5  b
                u4  b
                u6  c
                u2  c
                u1  b
                u2  a
                u2  a
                u3  a
                u5  a
                u5  a
                u5  a
请统计：
(1)每个店铺的UV（访客数）
(2)每个店铺访问次数top3的访客信息。输出店铺名称、访客id、访问次数

CREATE TABLE test_sql.test2 ( 
                         user_id string, 
                         shop string )
            ROW format delimited FIELDS TERMINATED BY '\t'; 
			
INSERT INTO TABLE test_sql.test2 VALUES
( 'u1', 'a' ),
( 'u2', 'b' ),
( 'u1', 'b' ),
( 'u1', 'a' ),
( 'u3', 'c' ),
( 'u4', 'b' ),
( 'u1', 'a' ),
( 'u2', 'c' ),
( 'u5', 'b' ),
( 'u4', 'b' ),
( 'u6', 'c' ),
( 'u2', 'c' ),
( 'u1', 'b' ),
( 'u2', 'a' ),
( 'u2', 'a' ),
( 'u3', 'a' ),
( 'u5', 'a' ),
( 'u5', 'a' ),
( 'u5', 'a' ); 

(1)
select shop,count(distinct user_id)
from test_sql.test2
group by shop;

方式2：
        SELECT t.shop,
               count(*)
        FROM
          (SELECT user_id,
                  shop
           FROM test_sql.test2
           GROUP BY user_id,
                    shop) t
        GROUP BY t.shop

(2)-1 我的SQL

select shop,user_id,user_count
from (
select 
shop,user_id,user_count,row_number() over(partition by shop order by user_count desc) seq
from (
select shop,user_id,count(user_id) user_count
from test_sql.test2
group by shop,user_id ) t1 ) t2
where t2.seq<=3

(2)-2 官方

SELECT t2.shop,
       t2.user_id,
       t2.cnt
FROM
  (SELECT t1.*,
          row_number() over(partition BY t1.shop
                            ORDER BY t1.cnt DESC) rank
   FROM
     (SELECT user_id,
             shop,
             count(*) AS cnt
      FROM test_sql.test2
      GROUP BY user_id,
               shop) t1)t2
WHERE rank <= 3



----------------------------------------------------------------------------------------------------------------------------------

第三题
已知一个表STG.ORDER，有如下字段:Date，Order_id，User_id，amount。
数据样例:2017-01-01,10029028,1000003251,33.57。
请给出sql进行统计:
(1)给出 2017年每个月的订单数、用户数、总成交金额。
(2)给出2017年11月的新客数(指在11月才有第一笔订单)

CREATE TABLE test_sql.test3 ( 
            dt string,
            order_id string, 
            user_id string, 
            amount DECIMAL ( 10, 2 ) )
ROW format delimited FIELDS TERMINATED BY '\t';
INSERT INTO TABLE test_sql.test3 VALUES ('2017-01-01','10029028','1000003251',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-01-01','10029029','1000003251',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-01-01','100290288','1000003252',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-02-02','10029088','1000003251',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-02-02','100290281','1000003251',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-02-02','100290282','1000003253',33.57);
INSERT INTO TABLE test_sql.test3 VALUES ('2017-11-02','10290282','100003253',234.00);
INSERT INTO TABLE test_sql.test3 VALUES ('2018-11-02','10290284','100003243',234.00);

(1)

1-1
select date_format(dt,'yyyy-MM') as `月份`,count(order_id) as `订单数`,count(distinct user_id) as `用户数`,sum(amount) as `总成交金额`
from test_sql.test3 where date_format(dt,'yyyy')='2017'
group by date_format(dt,'yyyy-MM');

1-2
SELECT t1.mon,
       count(t1.order_id) AS order_cnt,
       count(DISTINCT t1.user_id) AS user_cnt,
       sum(amount) AS total_amount
FROM
  (SELECT order_id,
          user_id,
          amount,
          date_format(dt,'yyyy-MM') mon
   FROM test_sql.test3
   WHERE date_format(dt,'yyyy') = '2017') t1
GROUP BY t1.mon

(2)
SELECT count(user_id)
FROM test_sql.test3
GROUP BY user_id
HAVING date_format(min(dt),'yyyy-MM')='2017-11';


----------------------------------------------------------------------------------------------------------------------------------
第四题
有一个5000万的用户文件(user_id，name，age)，一个2亿记录的用户看电影的记录文件(user_id，url)，根据年龄段观看电影的次数进行排序？ 

CREATE TABLE test_sql.test4user
           (user_id string,
            name string,
            age int);

CREATE TABLE test_sql.test4log
                        (user_id string,
                        url string);

INSERT INTO TABLE test_sql.test4user VALUES('001','u1',10);
INSERT INTO TABLE test_sql.test4user VALUES('002','u2',15);   
INSERT INTO TABLE test_sql.test4user VALUES('003','u3',15);   
INSERT INTO TABLE test_sql.test4user VALUES('004','u4',20);   
INSERT INTO TABLE test_sql.test4user VALUES('005','u5',25);   
INSERT INTO TABLE test_sql.test4user VALUES('006','u6',35);   
INSERT INTO TABLE test_sql.test4user VALUES('007','u7',40);
INSERT INTO TABLE test_sql.test4user VALUES('008','u8',45);  
INSERT INTO TABLE test_sql.test4user VALUES('009','u9',50);  
INSERT INTO TABLE test_sql.test4user VALUES('0010','u10',65);  
INSERT INTO TABLE test_sql.test4log VALUES('001','url1');
INSERT INTO TABLE test_sql.test4log VALUES('002','url1');   
INSERT INTO TABLE test_sql.test4log VALUES('003','url2');   
INSERT INTO TABLE test_sql.test4log VALUES('004','url3');   
INSERT INTO TABLE test_sql.test4log VALUES('005','url3');   
INSERT INTO TABLE test_sql.test4log VALUES('006','url1');   
INSERT INTO TABLE test_sql.test4log VALUES('007','url5');
INSERT INTO TABLE test_sql.test4log VALUES('008','url7');  
INSERT INTO TABLE test_sql.test4log VALUES('009','url5');  
INSERT INTO TABLE test_sql.test4log VALUES('0010','url1');

-- log表有2亿条，user表有5000万

-- 以下SQL对log表做了提前聚合优化，聚合出5000万，然后log表和user全关联，只有5000万条数据才做分组排序
SELECT 
t2.age_phase,
sum(t1.cnt) as view_cnt
FROM
(SELECT user_id,
  count(*) cnt
FROM test_sql.test4log
GROUP BY user_id) t1
JOIN
(SELECT user_id,
  CASE WHEN age <= 10 AND age > 0 THEN '0-10' 
  WHEN age <= 20 AND age > 10 THEN '10-20'
  WHEN age >20 AND age <=30 THEN '20-30'
  WHEN age >30 AND age <=40 THEN '30-40'
  WHEN age >40 AND age <=50 THEN '40-50'
  WHEN age >50 AND age <=60 THEN '50-60'
  WHEN age >60 AND age <=70 THEN '60-70'
  ELSE '70以上' END as age_phase
FROM test_sql.test4user) t2 ON t1.user_id = t2.user_id 
GROUP BY t2.age_phase


-- 以下SQL只是log表和user全关联，有两亿条数据才做的分组排序
select u.age_group,count(l.url) 
from test_sql.test4log l
left join (
select 
user_id,
case when age <=10 and age >0 then '0-10'
     when age > 10 and age <=20 then '10-20'
     when age > 20 and age <=30 then '20-30'
     when age > 30 and age <=40 then '30-40'
     when age > 40 and age <=50 then '40-50'
     when age > 50 and age <=60 then '50-60'
     else '70以上' end as age_group
from test_sql.test4user ) u 
on l.user_id=u.user_id
group by u.age_group
order by count(l.url) desc
----------------------------------------------------------------------------------------------------------------------------------
第五题
有日志如下，请写出代码求得所有用户和活跃用户的总数及平均年龄。（活跃用户指连续两天都有访问记录的用户）
日期 用户 年龄
2019-02-11,test_1,23
2019-02-11,test_2,19
2019-02-11,test_3,39
2019-02-11,test_1,23
2019-02-11,test_3,39
2019-02-11,test_1,23
2019-02-12,test_2,19
2019-02-13,test_1,23
2019-02-15,test_2,19
2019-02-16,test_2,19


CREATE TABLE test5(
dt string,
user_id string,
age int)
ROW format delimited fields terminated BY ',';
INSERT INTO TABLE test_sql.test5 VALUES ('2019-02-11','test_1',23);
INSERT INTO TABLE test_sql.test5 VALUES ('2019-02-11','test_2',19);
INSERT INTO TABLE test_sql.test5 VALUES ('2019-02-11','test_3',39);
INSERT INTO TABLE test_sql.test5 VALUES ('2019-02-11','test_1',23);
INSERT INTO TABLE test_sql.test5 VALUES ('2019-02-11','test_3',39);
INSERT INTO TABLE test_sql.test5 VALUES ('2019-02-11','test_1',23);
INSERT INTO TABLE test_sql.test5 VALUES ('2019-02-12','test_2',19);
INSERT INTO TABLE test_sql.test5 VALUES ('2019-02-13','test_1',23);
INSERT INTO TABLE test_sql.test5 VALUES ('2019-02-15','test_2',19);                                        
INSERT INTO TABLE test_sql.test5 VALUES ('2019-02-16','test_2',19); 

SELECT sum(total_user_cnt) total_user_cnt,
       sum(total_user_avg_age) total_user_avg_age,
       sum(two_days_cnt) two_days_cnt,
       sum(avg_age) avg_age
FROM
  (
  --活跃用户的总数及平均年龄
  SELECT  0 total_user_cnt,
          0 total_user_avg_age,
          count(*) AS two_days_cnt,
          cast(sum(age) / count(*) AS decimal(5,2)) AS avg_age
   FROM
     (SELECT user_id,
             max(age) age
      FROM
        (
		 -- 最后根据userid聚合，得到活跃用户的总数，而不是活跃次数
		 SELECT user_id,
                max(age) age
         FROM
           (
		    -- 关键算法，分组后数据进行开窗函数user_id分组dt排序
			--           日期减去排序后的日期的序列号，得到初始日期，初始日志进行count()聚合，根据count()聚合后的值来筛选连续几天
			--           如果聚合后的count值 >=2 表示连续两天以上
			SELECT user_id,
                   age,
                   date_sub(dt,rank) flag
            FROM
              (SELECT dt,
                      user_id,
                      max(age) age,
                      row_number() over(PARTITION BY user_id ORDER BY dt) rank
               FROM test_sql.test5
               GROUP BY dt,user_id) t1
			) t2
         GROUP BY user_id,flag
         HAVING count(*) >=2
	  ) t3
      GROUP BY user_id
	 ) t4
   UNION ALL 
   -- 所有用户的总数及平均年龄
   SELECT count(*) total_user_cnt,
          cast(sum(age) /count(*) AS decimal(5,2)) total_user_avg_age,
          0 two_days_cnt,
          0 avg_age
   FROM
     (SELECT user_id,
             max(age) age
      FROM test_sql.test5
      GROUP BY user_id) t5) t6

----------------------------------------------------------------------------------------------------------------------------------

第六题
请用sql写出所有用户中在今年10月份第一次购买商品的金额，
表ordertable字段:
(购买用户：userid，金额：money，购买时间：paymenttime(格式：2017-10-01)，订单id：orderid  

CREATE TABLE test_sql.test6 (
        userid string,
        money decimal(10,2),
        paymenttime string,
        orderid string);

INSERT INTO TABLE test_sql.test6 VALUES('001',100.00,'2017-10-01','123');
INSERT INTO TABLE test_sql.test6 VALUES('001',200.00,'2017-10-02','124');
INSERT INTO TABLE test_sql.test6 VALUES('002',500.00,'2017-10-01','125');
INSERT INTO TABLE test_sql.test6 VALUES('001',100.00,'2017-11-01','126');

select t.userid,t.paymenttime,t.orderid,t.money from(
select userid,money,paymenttime,orderid,row_number() over(partition by userid order by date_format(paymenttime,'yyyy-MM') asc) as seq
from test_sql.test6
where 
date_format(paymenttime,'yyyy-MM')='2017-10') t where t.seq=1

----------------------------------------------------------------------------------------------------------------------------------
第七题

现有图书管理数据库的三个数据模型如下：
图书（数据表名：BOOK）
    序号      字段名称    字段描述    字段类型
    1       BOOK_ID     总编号         文本
    2       SORT        分类号         文本
    3       BOOK_NAME   书名           文本
    4       WRITER      作者           文本
    5       OUTPUT      出版单位       文本
    6       PRICE       单价           数值（保留小数点后2位）
读者（数据表名：READER）
    序号      字段名称    字段描述    字段类型
    1       READER_ID   借书证号       文本
    2       COMPANY     单位           文本
    3       NAME        姓名           文本
    4       SEX         性别           文本
    5       GRADE       职称           文本
    6       ADDR        地址           文本
借阅记录（数据表名：BORROW LOG）
    序号      字段名称        字段描述    字段类型
    1       READER_ID       借书证号       文本
    2       BOOK_ID         总编号         文本
    3       BORROW_DATE     借书日期       日期
（1）创建图书管理库的图书、读者和借阅三个基本表的表结构。请写出建表语句。
（2）找出姓李的读者姓名（NAME）和所在单位（COMPANY）。
（3）查找“高等教育出版社”的所有图书名称（BOOK_NAME）及单价（PRICE），结果按单价降序排序。
（4）查找价格介于10元和20元之间的图书种类(SORT）出版单位（OUTPUT）和单价（PRICE），结果按出版单位（OUTPUT）和单价（PRICE）升序排序。
（5）查找所有借了书的读者的姓名（NAME）及所在单位（COMPANY）。
（6）求”科学出版社”图书的最高单价、最低单价、平均单价。
（7）找出当前至少借阅了2本图书（大于等于2本）的读者姓名及其所在单位。
（8）考虑到数据安全的需要，需定时将“借阅记录”中数据进行备份，请使用一条SQL语句，在备份用户bak下创建与“借阅记录”表结构完全一致的数据表BORROW_LOG_BAK.井且将“借阅记录”中现有数据全部复制到BORROW_L0G_ BAK中。
（9）现在需要将原Oracle数据库中数据迁移至Hive仓库，请写出“图书”在Hive中的建表语句（Hive实现，提示：列分隔符|；数据表数据需要外部导入：分区分别以month＿part、day＿part 命名）
（10）Hive中有表A，现在需要将表A的月分区　201505　中　user＿id为20000的user＿dinner字段更新为bonc8920，其他用户user＿dinner字段数据不变，请列出更新的方法步骤。（Hive实现，提示：Hlive中无update语法，请通过其他办法进行数据更新）

-- 创建图书表book

CREATE TABLE test_sql.book(book_id string,
                           `SORT` string,
                           book_name string,
                           writer string,
                           OUTPUT string,
                           price decimal(10,2));
INSERT INTO TABLE test_sql.book VALUES ('001','TP391','信息处理','author1','机械工业出版社',20.00);
INSERT INTO TABLE test_sql.book VALUES ('002','TP392','数据库','author12','科学出版社',15.00);
INSERT INTO TABLE test_sql.book VALUES ('003','TP393','计算机网络','author3','机械工业出版社',29.00);
INSERT INTO TABLE test_sql.book VALUES ('004','TP399','微机原理','author4','科学出版社',39.00);
INSERT INTO TABLE test_sql.book VALUES ('005','C931','管理信息系统','author5','机械工业出版社',40.00);
INSERT INTO TABLE test_sql.book VALUES ('006','C932','运筹学','author6','科学出版社',55.00);


-- 创建读者表reader

CREATE TABLE test_sql.reader (reader_id string,
                              company string,
                              name string,
                              sex string,
                              grade string,
                              addr string);
INSERT INTO TABLE test_sql.reader VALUES ('0001','阿里巴巴','jack','男','vp','addr1');
INSERT INTO TABLE test_sql.reader VALUES ('0002','百度','robin','男','vp','addr2');
INSERT INTO TABLE test_sql.reader VALUES ('0003','腾讯','tony','男','vp','addr3');
INSERT INTO TABLE test_sql.reader VALUES ('0004','京东','jasper','男','cfo','addr4');
INSERT INTO TABLE test_sql.reader VALUES ('0005','网易','zhangsan','女','ceo','addr5');
INSERT INTO TABLE test_sql.reader VALUES ('0006','搜狐','lisi','女','ceo','addr6');

-- 创建借阅记录表borrow_log

CREATE TABLE test_sql.borrow_log(reader_id string,
                                 book_id string,
                                 borrow_date string);

INSERT INTO TABLE test_sql.borrow_log VALUES ('0001','002','2019-10-14');
INSERT INTO TABLE test_sql.borrow_log VALUES ('0002','001','2019-10-13');
INSERT INTO TABLE test_sql.borrow_log VALUES ('0003','005','2019-09-14');
INSERT INTO TABLE test_sql.borrow_log VALUES ('0004','006','2019-08-15');
INSERT INTO TABLE test_sql.borrow_log VALUES ('0005','003','2019-10-10');
INSERT INTO TABLE test_sql.borrow_log VALUES ('0006','004','2019-17-13');

-- 我的SQL
（2）找出姓李的读者姓名（NAME）和所在单位（COMPANY）。
select name,company from reader where name like 'li%'

（3）查找“高等教育出版社”的所有图书名称（BOOK_NAME）及单价（PRICE），结果按单价降序排序。
select book_name,price from book where output ='高等教育出版社' order by price desc

（4）查找价格介于10元和20元之间的图书种类(SORT）出版单位（OUTPUT）和单价（PRICE），结果按出版单位（OUTPUT）和单价（PRICE）升序排序。
select `sort`,output,price from book where price>=10 and price<=20 order by output,price 

（5）查找所有借了书的读者的姓名（NAME）及所在单位（COMPANY）。
select r.name,r.company from borrow_log bl 
join reader r on bl.reader_id=r.reader_id

（6）求”科学出版社”图书的最高单价、最低单价、平均单价。
select max(price),min(price),avg(price) from book where output='科学出版社'

（7）找出当前至少借阅了2本图书（大于等于2本）的读者姓名及其所在单位。
-- 能查出数据就行,先不要考虑SQL优化问题
select t1.name,t1.company from reader t1 join
(
select reader_id,count(reader_id) cnt_ids from borrow_log where borrow_date='2019-10-14' group by reader_id 
) t2 on t1.reader_id = t2.reader_id and t2.cnt_ids>=2

-- 聚合函数不能在where条件中，得在having条件中
select t1.name,t1.company from reader t1 join
(
select reader_id from borrow_log where borrow_date='2019-10-14' group by reader_id having count(reader_id)>=2
) t2 on t1.reader_id = t2.reader_id

-- 错误案例
select t.reader_id from
(
select r.name,r.company,bl.reader_id from borrow_log bl 
join reader r on (bl.reader_id=r.reader_id) where bl.borrow_date='2019-10-14'
) t
group by t.reader_id 
having count(t.reader_id)>=2


（8）考虑到数据安全的需要，需定时将“借阅记录”中数据进行备份，请使用一条SQL语句，在备份用户bak下创建与“借阅记录”表结构完全一致的数据表BORROW_LOG_BAK.井且将“借阅记录”中现有数据全部复制到BORROW_L0G_ BAK中。
create table test_sql.borrow_log_bak as select * from borrow_log

select * from test_sql.borrow_log_bak

（9）现在需要将原Oracle数据库中数据迁移至Hive仓库，请写出“图书”在Hive中的建表语句（Hive实现，提示：列分隔符|；数据表数据需要外部导入：分区分别以month＿part、day＿part 命名）
create table test_sql.book_oracle
(
book_id int,
`sort` string,
book_name string,
writer string,
optput string,
price decimal(10,2)
)
partitioned by(month_part string,day_part string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE; 

describe test_sql.book_oracle
show create table test_sql.book_oracle


（10）Hive中有表A，现在需要将表A的月分区 201505 中 user＿id为20000的user＿dinner字段更新为bonc8920，其他用户user＿dinner字段数据不变，请列出更新的方法步骤。（Hive实现，提示：Hlive中无update语法，请通过其他办法进行数据更新）
create table tmp1 as select 'bonc8920' user_dinner from A where month_part='201505' and user_id='20000'
insert into tmp1 as select * from A where month_part='201505' and user_id not in('20000')
alter table A drop partition(month='201505')
alter table A add partition(month='201505')
insert into A partition(month='201505') as select * from tmp1

-- 官方SQL
(2)
    SELECT name,
           company
    FROM test_sql.reader
    WHERE name LIKE '李%';
(3)
    SELECT book_name,
           price
    FROM test_sql.book
    WHERE OUTPUT = "高等教育出版社"
    ORDER BY price DESC;
(4)
    SELECT sort,
           output,
           price
    FROM test_sql.book
    WHERE price >= 10 and price <= 20
    ORDER BY output,price ;
(5)
    SELECT b.name,
           b.company
    FROM test_sql.borrow_log a
    JOIN test_sql.reader b ON a.reader_id = b.reader_id;
(6)
    SELECT max(price),
           min(price),
           avg(price)
    FROM test_sql.book
    WHERE OUTPUT = '科学出版社';
(7)
    SELECT b.name,
           b.company
    FROM
      (SELECT reader_id
       FROM test_sql.borrow_log
       GROUP BY reader_id
       HAVING count(*) >= 2) a
    JOIN test_sql.reader b ON a.reader_id = b.reader_id;

(8)
    CREATE TABLE test_sql.borrow_log_bak AS
    SELECT *
    FROM test_sql.borrow_log;
(9)
    CREATE TABLE book_hive ( 
    book_id string,
    SORT string, 
    book_name string,
    writer string, 
    OUTPUT string, 
    price DECIMAL ( 10, 2 ) )
    partitioned BY ( month_part string, day_part string )
    ROW format delimited FIELDS TERMINATED BY '\\|' stored AS textfile;
(10)
    方式1：配置hive支持事务操作，分桶表，orc存储格式
    方式2：第一步找到要更新的数据，将要更改的字段替换为新的值，第二步找到不需要更新的数据，第三步将上两步的数据插入一张新表中。


----------------------------------------------------------------------------------------------------------------------------------

第八题

有一个线上服务器访问日志格式如下（用sql答题）
时间                    接口                         ip地址
2016-11-09 14:22:05        /api/user/login             110.23.5.33
2016-11-09 14:23:10        /api/user/detail            57.3.2.16
2016-11-09 15:59:40        /api/user/login             200.6.5.166
… …
求11月9号下午14点（14-15点），访问/api/user/login接口的top10的ip地址


CREATE TABLE test_sql.test8(`date` string,
                interface string,
                ip string);

INSERT INTO TABLE test_sql.test8 VALUES ('2016-11-09 11:22:05','/api/user/login','110.23.5.23');
INSERT INTO TABLE test_sql.test8 VALUES ('2016-11-09 11:23:10','/api/user/detail','57.3.2.16');
INSERT INTO TABLE test_sql.test8 VALUES ('2016-11-09 23:59:40','/api/user/login','200.6.5.166');
INSERT INTO TABLE test_sql.test8 VALUES('2016-11-09 11:14:23','/api/user/login','136.79.47.70');
INSERT INTO TABLE test_sql.test8 VALUES('2016-11-09 11:15:23','/api/user/detail','94.144.143.141');
INSERT INTO TABLE test_sql.test8 VALUES('2016-11-09 11:16:23','/api/user/login','197.161.8.206');
INSERT INTO TABLE test_sql.test8 VALUES('2016-11-09 12:14:23','/api/user/detail','240.227.107.145');
INSERT INTO TABLE test_sql.test8 VALUES('2016-11-09 13:14:23','/api/user/login','79.130.122.205');
INSERT INTO TABLE test_sql.test8 VALUES('2016-11-09 14:14:23','/api/user/detail','65.228.251.189');
INSERT INTO TABLE test_sql.test8 VALUES('2016-11-09 14:15:23','/api/user/detail','245.23.122.44');
INSERT INTO TABLE test_sql.test8 VALUES('2016-11-09 14:17:23','/api/user/detail','22.74.142.137');
INSERT INTO TABLE test_sql.test8 VALUES('2016-11-09 14:19:23','/api/user/detail','54.93.212.87');
INSERT INTO TABLE test_sql.test8 VALUES('2016-11-09 14:20:23','/api/user/detail','218.15.167.248');
INSERT INTO TABLE test_sql.test8 VALUES('2016-11-09 14:24:23','/api/user/detail','20.117.19.75');
INSERT INTO TABLE test_sql.test8 VALUES('2016-11-09 15:14:23','/api/user/login','183.162.66.97');
INSERT INTO TABLE test_sql.test8 VALUES('2016-11-09 16:14:23','/api/user/login','108.181.245.147');
INSERT INTO TABLE test_sql.test8 VALUES('2016-11-09 14:17:23','/api/user/login','22.74.142.137');
INSERT INTO TABLE test_sql.test8 VALUES('2016-11-09 14:19:23','/api/user/login','22.74.142.137');

select ip,count(ip) cnt from test_sql.test8 
where 
unix_timestamp(`date`) >= unix_timestamp('2016-11-09 14:00:00') 
and unix_timestamp(`date`) <= unix_timestamp('2016-11-09 15:00:00') 
and interface='/api/user/login'
group by ip
order by cnt desc
limit 10;

----------------------------------------------------------------------------------------------------------------------------------
第九题

有一个充值日志表credit_log，字段如下：

`dist_id` int  '区组id',
`account` string  '账号',
`money` int   '充值金额',
`create_time` string  '订单时间'

请写出SQL语句，查询充值日志表2019年01月02号每个区组下充值额最大的账号，要求结果：
区组id，账号，金额，充值时间 

CREATE TABLE test_sql.test9(
            dist_id string COMMENT '区组id',
            account string COMMENT '账号',
           `money` decimal(10,2) COMMENT '充值金额',
            create_time string COMMENT '订单时间');

INSERT INTO TABLE test_sql.test9 VALUES ('1','11',100006.00,'2019-01-02 13:00:01');
INSERT INTO TABLE test_sql.test9 VALUES ('1','22',110000.00,'2019-01-02 13:00:02');
INSERT INTO TABLE test_sql.test9 VALUES ('1','33',102000.00,'2019-01-02 13:00:03');
INSERT INTO TABLE test_sql.test9 VALUES ('1','44',100300.00,'2019-01-02 13:00:04');
INSERT INTO TABLE test_sql.test9 VALUES ('1','55',100040.00,'2019-01-02 13:00:05');
INSERT INTO TABLE test_sql.test9 VALUES ('1','66',100005.00,'2019-01-02 13:00:06');
INSERT INTO TABLE test_sql.test9 VALUES ('1','77',180000.00,'2019-01-03 13:00:07');
INSERT INTO TABLE test_sql.test9 VALUES ('1','88',106000.00,'2019-01-02 13:00:08');
INSERT INTO TABLE test_sql.test9 VALUES ('1','99',100400.00,'2019-01-02 13:00:09');
INSERT INTO TABLE test_sql.test9 VALUES ('1','12',100030.00,'2019-01-02 13:00:10');
INSERT INTO TABLE test_sql.test9 VALUES ('1','13',100003.00,'2019-01-02 13:00:20');
INSERT INTO TABLE test_sql.test9 VALUES ('1','14',100020.00,'2019-01-02 13:00:30');
INSERT INTO TABLE test_sql.test9 VALUES ('1','15',100500.00,'2019-01-02 13:00:40');
INSERT INTO TABLE test_sql.test9 VALUES ('1','16',106000.00,'2019-01-02 13:00:50');
INSERT INTO TABLE test_sql.test9 VALUES ('1','17',100800.00,'2019-01-02 13:00:59');
INSERT INTO TABLE test_sql.test9 VALUES ('2','18',100800.00,'2019-01-02 13:00:11');
INSERT INTO TABLE test_sql.test9 VALUES ('2','19',100030.00,'2019-01-02 13:00:12');
INSERT INTO TABLE test_sql.test9 VALUES ('2','10',100000.00,'2019-01-02 13:00:13');
INSERT INTO TABLE test_sql.test9 VALUES ('2','45',100010.00,'2019-01-02 13:00:14');
INSERT INTO TABLE test_sql.test9 VALUES ('2','78',100070.00,'2019-01-02 13:00:15');  

unix_timestamp(string str)               str       必须是yyyy-MM-dd HH:mm:ss格式的字符串,返回的是时间戳
from_unixtime(时间戳,string formatstr)   formatstr 表示时间的格式化的格式，例如yyyy-MM-dd HH:mm:ss

select from_unixtime(unix_timestamp(create_time),'yyyy-MM-dd HH:mm:ss') from test_sql.test9

hive独有函数：
date_format(string str,string formatstr) 
str       必须是yyyy-MM-dd HH:mm:ss格式的字符串,返回的是时间戳
formatstr 表示时间的格式化的格式，例如yyyy-MM-dd HH:mm:ss

select data_format(create_time,'yyyy-MM-dd HH') from test_sql.test9

select t.* from(
select dist_id,account,money,from_unixtime(unix_timestamp(create_time),'yyyy-MM-dd'),
row_number() over(partition by dist_id order by money desc) seq 
from test_sql.test9 where from_unixtime(unix_timestamp(create_time),'yyyy-MM-dd')='2019-01-02' ) t where t.seq=1

with as子句
WITH TEMP AS
  (SELECT dist_id,
          account,
          sum(`money`) sum_money
   FROM test_sql.test9
   WHERE date_format(create_time,'yyyy-MM-dd') = '2019-01-02'
   GROUP BY dist_id,
            account)
SELECT t1.dist_id,
       t1.account,
       t1.sum_money
FROM
  (SELECT temp.dist_id,
          temp.account,
          temp.sum_money,
          rank() over(partition BY temp.dist_id
                      ORDER BY temp.sum_money DESC) ranks
   FROM TEMP) t1
WHERE ranks = 1
----------------------------------------------------------------------------------------------------------------------------------
第十题

有一个账号表如下，请写出SQL语句，查询各自区组的money排名前十的账号（分组取前10）
dist_id string  '区组id',
account string  '账号',
gold     int    '金币' 

CREATE TABLE test_sql.test10(
    `dist_id` string COMMENT '区组id',
    `account` string COMMENT '账号',
    `gold` int COMMENT '金币'
);

INSERT INTO TABLE test_sql.test10 VALUES ('1','77',18);
INSERT INTO TABLE test_sql.test10 VALUES ('1','88',106);
INSERT INTO TABLE test_sql.test10 VALUES ('1','99',10);
INSERT INTO TABLE test_sql.test10 VALUES ('1','12',13);
INSERT INTO TABLE test_sql.test10 VALUES ('1','13',14);
INSERT INTO TABLE test_sql.test10 VALUES ('1','14',25);
INSERT INTO TABLE test_sql.test10 VALUES ('1','15',36);
INSERT INTO TABLE test_sql.test10 VALUES ('1','16',12);
INSERT INTO TABLE test_sql.test10 VALUES ('1','17',158);
INSERT INTO TABLE test_sql.test10 VALUES ('2','18',12);
INSERT INTO TABLE test_sql.test10 VALUES ('2','19',44);
INSERT INTO TABLE test_sql.test10 VALUES ('2','10',66);
INSERT INTO TABLE test_sql.test10 VALUES ('2','45',80);
INSERT INTO TABLE test_sql.test10 VALUES ('2','78',98);  


SELECT dist_id,
   account,
   gold
FROM
(SELECT dist_id,
      account,
      gold,
      row_number () over (PARTITION BY dist_id
                          ORDER BY gold DESC) rank
FROM test_sql.test10) t
WHERE rank <= 10
