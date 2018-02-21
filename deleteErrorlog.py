# - *- coding:utf-8 -*-

'''
删除115服务器上各个库里的 tblmetererror表中errortime在2017-11-1之前的所有数据
即清除历史错误日志
'''
import pymssql
from sqlconfig import config
import sqlmethod as sm
import time


conn=pymssql.connect(**config["UserLocal"])
cur=conn.cursor()

#1.查询115 服务器上所有数据库的名字
sql=sm.allbase()
cur.execute(sql)
baselist=cur.fetchall()
baselist=[base[0] for base in baselist]
print(baselist)


#2.查询数据库中是否有表

sql2=sm.sbasetable(baselist,'tblmetererror')
print(sql2)

cur.execute(sql2)
mbase=[]
for i in range(len(baselist)):
    datacol=cur.fetchall()
    if len(datacol)>0:
        mbase.append(datacol[0][0])

print(len(mbase),mbase)



#3.查询各个数据库中该表里符合条件的记录条数
sql3=sm.sl(['tblmetererror',],"where errortime<'2017-11-01 '",'count(pid)',mbase)
print(sql3)


cur.execute(sql3)
columns={}

for i in range(len(mbase)):
    columns[mbase[i]]=cur.fetchall()[0]


sortdata=sorted(columns.items(),key=lambda item:item[1])
print(sortdata)

#4.根据每个数据库中符合条件的记录数，每次删20000条，返回应删除的次数(考虑到部分库中原始数据较大分批次删除)
adddata=[(ii,j[0],j[0]//20000+1) for ii,j in sortdata]
print(adddata)
sqllist=[]
 #删除完后执行到这里查看每个数据库中符合条件的记录数是否为0 后即可推出

for i,j,k in adddata:
    # 在该数据库中满足条件的记录不为0 时，输出对应次数的删除语句
    if j>0:
        while k>0:
            sqllist.append('''use %s 
            delete from tblmetererror 
            where pid in (select top 20000 pid from   tblmetererror where errortime<'2017-11-01 ')'''%i)
            k=k-1


#5. 删除语句10条一执行
for i in range(0, len(sqllist)//10*10, 10):
    print('\n'.join(sqllist[i:i+10]))
    cur.execute('\n'.join(sqllist[i:i+10]))
    conn.commit()
    time.sleep(2)
print('\n'.join(sqllist[len(sqllist)//10*10:]))
cur.execute('\n'.join(sqllist[len(sqllist)//10*10:]))
conn.commit()

cur.close()
conn.close()
