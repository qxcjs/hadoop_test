左连接，两个文件分别代表2个表,连接字段table1的id字段和table2的cityID字段
table1(左表):tb_dim_city(id int,name string,orderid int,city_code,is_show)
```
 id     name  orderid  city_code  is_show
 1       长春        1        901          1
 2       吉林        2        902          1
 3       四平        3        903          1
 4       松原        4        904          1
 5       通化        5        905          1
 6       辽源        6        906          1
 7       白城        7        907          1
 8       白山        8        908          1
 9       延吉        9        909          1
```

table2(右表)：tb_user_profiles(userID int,userName string,network string,double flow,cityID int)
```
 userID   network     flow    cityID
 1           2G       123      1
 2           3G       333      2
 3           3G       555      1
 4           2G       777      3
 5           3G       666      4
```

结果
```
city
 1   长春  1   901 1   1   2G  123
 1   长春  1   901 1   3   3G  555
 2   吉林  2   902 1   2   3G  333
 3   四平  3   903 1   4   2G  777
 4   松原  4   904 1   5   3G  666
```