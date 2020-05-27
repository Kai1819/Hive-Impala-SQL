Hive函数

sum(a) over(partition by b order by d)
通过对b分组,每组内排序d,累计聚合a,生成聚合后的值

row_number() over(partition by a order by b)
通过对a分组,每组内排序d,生成排序号,不考虑并列值

rank() over(partition by a order by b)
通过对a分组,每组内排序d,生成排序号,相同列值并列显示,如两个为4的排序,然后接下来就是排序为6,其中排名5跳过。

dense_rank() over(partition by a order by b)
通过对a分组,每组内排序d,生成排序号,相同列值并列显示,如两个为4的排序,然后接下来就是排序为5,为密集排序。


Hive的作为临时表查询
with 表名 as () sql语句

union：对两个结果集进行并集操作，不包括重复行，同时进行默认规则的排序；
union all：对两个结果集进行并集操作，包括重复行，不进行排序

regexp_replace(STRING initial_string, STRING pattern, STRING replacement)
Returns the string resulting from replacing all substrings in INITIAL_STRING that match the java regular expression syntax defined in PATTERN with instances of REPLACEMENT. 
For example, regexp_replace("foobar", "oo|ar", "") returns 'fb.' Note that some care is necessary in using predefined character classes: using '\s' as the second argument will match the letter s; '\\s' is necessary to match whitespace, etc.

cast(a as T)
Converts the results of the expression expr to type T. For example, cast('1' as BIGINT) will convert the string '1' to its integral representation. 
A null is returned if the conversion does not succeed. If cast(expr as boolean) Hive returns true for a non-empty string.

trim(STRING a)
Returns the string resulting from trimming spaces from both ends of A. For example, trim(' foobar ') results in 'foobar'

substr(STRING|BINARY A, INT start [, INT len])
Returns the substring or slice of the byte array of A starting from start position till the end of string A or with optional length len. 
For example, substr('foobar', 4) results in 'bar'

replace(STRING a, STRING old, STRING new)
Returns the string a with all non-overlapping occurrences of old replaced with new (as of Hive 1.3.0 and 2.1.0). 
Example: select replace("ababab", "abab", "Z"); returns "Zab".

date_format(DATE|TIMESTAMP|STRING ts, STRING fmt)
Converts a date/timestamp/string to a value of string in the format specified by the date format fmt (as of Hive 1.2.0). 
Supported formats are Java SimpleDateFormat formats – https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html. 
The second argument fmt should be constant. Example: date_format('2015-04-08', 'y') = '2015'.
For example,date_format('2020-05-27 13:51:00','yyyy-MM-dd HH:mm:ss')

unix_timestamp([STRING date [, STRING pattern]])
Convert time string with given pattern to Unix time stamp (in seconds), 
return 0 if fail: unix_timestamp('2009-03-20', 'yyyy-MM-dd') = 1237532400.

from_unixtime(BIGINT unixtime [, STRING format])
Converts time string in format yyyy-MM-dd HH:mm:ss to Unix timestamp (in seconds), using the default timezone and the default locale, 
return 0 if fail: unix_timestamp('2009-03-20 11:30:01') = 1237573801

unix_timestamp 可以指定日期格式化格式，来转换为相应的时间戳
from_unixtime  只接受yyyy-MM-dd HH:mm:ss固定格式的时间字符串，来转换为相应的时间戳


简单case函数
case a 
     when '1' then '男' 
     when '2' then '女'
     else '其它' end
as sex

case搜索函数
case when a<50 then '<50'
	 when a>50 and a<100 then '50-100'
	 else '>100' end
as age

简单Case函数
CASE sex
WHEN '1' THEN '男'
WHEN '2' THEN '女'
ELSE '其他' END

Case搜索函数 
CASE WHEN sex = '1' THEN '男' 
WHEN sex = '2' THEN '女' 
ELSE '其他' END  
Case具有两种格式。简单Case函数和Case搜索函数。
简单Case函数的写法相对比较简洁，但是和Case搜索函数相比，功能方面会有些限制，比如写判断式。
还有一个需要注意的问题，Case函数只返回第一个符合条件的值，剩下的Case部分将会被自动忽略。

collect_set()  分组的字段组合成数组返回并对分组的字段去重，返回值是set集合
collect_set(col)
Returns a set of objects with duplicate elements eliminated.

collect_list() 分组的字段组合成数组返回,分组的字段不去重
collect_list(col)
Returns a list of objects with duplicates. (As of Hive 0.13.0.)


concat(string|binary A, string|binary B...) 对二进制字节码或字符串按次序进行拼接
concat(STRING|BINARY a, STRING|BINARY b...)
Returns the string or bytes resulting from concatenating the strings or bytes passed in as parameters in order. 
For example, concat('foo', 'bar') results in 'foobar'. Note that this function can take any number of input strings.

concat_ws(separator,str1,str2,…) 拼接Array中的元素并用指定分隔符进行分隔
concat_ws(STRING sep, STRING a, STRING b...), concat_ws(STRING sep, Array<STRING>)
Like concat(), but with custom separator SEP.
For example:
concat_ws(',',collect_set(create_time))

transform(col1,clo2,...) using 'python python文件名' 类似于UDF函数，使用python脚本处理transform传入的参数，并返回
For example:
select transform(msg_id,token_md5,time_list,action_list) 
using 'python show_click.py' as msg_id,token_md5,receipt_time,is_show,is_click

string() 其它字段转换为字符串

if(boolean,str1,str2) 函数，类似java中的a==1?1:2
if(BOOLEAN testCondition, T valueTrue, T valueFalseOrNull)
Returns valueTrue when testCondition is true, returns valueFalseOrNull otherwise.

length(STRING a)
Returns the length of the string.