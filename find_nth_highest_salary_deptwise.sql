-- Find Nth highest salary department wise -- 

create database employee;

use employee;

create table emp (
emp_id int,
emp_name char(10),
emp_dept char(10),
emp_salary int
);

insert  into emp (emp_id, emp_name, emp_dept, emp_salary) values(1,"john","finance",40000) ;
insert  into emp (emp_id, emp_name, emp_dept, emp_salary) values(2,"jerry","finance",20000) ;
insert  into emp (emp_id, emp_name, emp_dept, emp_salary) values(3,"mat","finance",1000) ;
insert  into emp (emp_id, emp_name, emp_dept, emp_salary) values(4,"ryan","hr",5000) ;
insert  into emp (emp_id, emp_name, emp_dept, emp_salary) values(5,"steve","hr",75000) ;
insert  into emp (emp_id, emp_name, emp_dept, emp_salary) values(6,"ted","it",87640) ;
insert  into emp (emp_id, emp_name, emp_dept, emp_salary) values(7,"joshua","it",8672) ;
insert  into emp (emp_id, emp_name, emp_dept, emp_salary) values(8,"george","hr",11000) ;
insert  into emp (emp_id, emp_name, emp_dept, emp_salary) values(9,"harry","hr",11000) ;

select * from emp;

/*
# emp_id, emp_name, emp_dept, emp_salary
1, john, finance, 40000
2, jerry, finance, 20000
3, mat, finance, 1000
4, ryan, hr, 5000
5, steve, hr, 75000
6, ted, it, 87640
7, joshua, it, 8672
8, george, hr, 11000
*/


select emp_name,emp_dept,emp_salary from(
select *, dense_rank()
over(partition by emp_dept  order by emp_salary  desc) as dr
from emp) x where x.dr = 2  ;

/*
# emp_name, emp_dept, emp_salary
jerry, finance, 20000
george, hr, 11000
joshua, it, 8672
*/

-----------------------------------------------------------------------------------------------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val df1 = Seq((1,"john","finance",40000),(2,"jerry","finance",20000),(3,"mat","finance",1000),(4,"ryan","hr",5000),(5,"steve","hr",75000),(6,"ted","it",87640),(7,"joshua","it",8672),(8,"george","hr",11000))
val df2 = df1.toDF("emp_id","emp_name","emp_dept","emp_salary")

val windowSpec = Window.partitionBy("emp_dept").orderBy($"emp_salary".desc)

df2.withColumn("dense_rank", dense_rank.over(windowSpec)).filter(col("dense_rank") === 2).select("emp_name","emp_dept","emp_salary").show(false)

df2.show(false)
/*
+------+--------+--------+----------+
|emp_id|emp_name|emp_dept|emp_salary|
+------+--------+--------+----------+
|1     |john    |finance |40000     |
|2     |jerry   |finance |20000     |
|3     |mat     |finance |1000      |
|4     |ryan    |hr      |5000      |
|5     |steve   |hr      |75000     |
|6     |ted     |it      |87640     |
|7     |joshua  |it      |8672      |
|8     |george  |hr      |11000     |
+------+--------+--------+----------+
*/


df2.withColumn("dense_rank", dense_rank.over(windowSpec)).filter(col("dense_rank") === 2).select("emp_name","emp_dept","emp_salary").show(false)

/*
+--------+--------+----------+
|emp_name|emp_dept|emp_salary|
+--------+--------+----------+
|jerry   |finance |20000     |
|george  |hr      |11000     |
|joshua  |it      |8672      |
+--------+--------+----------+
*/
