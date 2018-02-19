#-*- coding:utf-8 -*-
"""
水表入库，excel数据导入数据库,sql语句更新
"""
import re
import pandas as pd
import numpy as np
import sys
import json 
import pymssql
import datetime 
from msconfig import cfg
from sqlcode import sqldic

#sys.exit()

def data_get(file,sheet='源'):
    """
数据源获取,返回原始pd即data，和 去重后的（filialeid，unit）组成的pd"""
    #1.读取excel中的数据
    celinfo={
    'io':file
    ,'sheetname':sheet
    ,'header':0   #0表示跳过的行数后的第一行，数据从header后的下一行开始
    ,'skiprows':0 #0表示不跳过
    ,'skip_footer':0
    # ,'converters':{'装卡日期':lambda x:str(x),}
    ,'parse_cols':'A,C:M'#解析列的数据
    ,'convert_float':True
    }
    data11=pd.read_excel(**celinfo)
    print(data11.columns)
    num=data11.loc[0]
    print("({0}','{1}','{2}','{3}','{4}','{5}') ".format(num[5],num[2],num[1],num[7],num[9],num[0]))
    return data11.groupby('Fmfiliale')

def insert_to(data,base):
    """
源数据，导入数据库,返回数据 库连接conn,cur"""
    conn=pymssql.connect(**base)
    cur=conn.cursor()
    sql=sqldic["insert_sql"]
    addmeter=sqldic["insert_addmeter"]
    creat=sqldic["insert_create"]
    try:
        cur.execute(sql)
    except:
        cur.execute(creat)
    columns=[]
    if data.shape==(11,):
        meter=re.match(r'(MAG\d{6,7})[（(](\d{2,3})mm[）)]',data[7])
        sql2=sqldic["insert_sqk2"].format(data[5],data[2],data[1],data[7],meter.group(1),meter.group(2),data[9],data[0])
        try:
            cur.execute(sql2)
        except:
            cur.execute(addmeter)
            cur.execute(sql2)
    else:
        for i in data.index:
            num=data.loc[i]
            meter=re.match(r'(MAG\d{6,7})[（(](\d{2,3})mm[）)]',num[7])
            if meter==None:
                print(num[7])

            sql2=sqldic["insert_sql22"].format(num[5],num[2],num[1],num[7],meter.group(1),meter.group(2),num[9],num[0])
            try:
                cur.execute(sql2)
            except:
                cur.execute(addmeter)
                cur.execute(sql2)
    conn.commit()
    return conn,cur

def update_to(conn,cur):
    """
更新数据库中数据,返回更新的userid列表"""
    #1.更新临时表中的数据格式
    sql1=sqldic["update_sql1"]
    cur.execute(sql1)
    inNum=cur.fetchone()
    print('tmp data number: ----',inNum[0])
    #2.查询将被写入tbluserinfo中的表编码、filialename
    sql2_1=sqldic["update_sql2_1"]
    cur.execute(sql2_1)
    fmname=cur.fetchall()
    filiale=set([i[1] for i in fmname if fmname])
    print('###---------￥￥￥￥---distinnct filialename is :',filiale)
    usrid=[i[0] for i in fmname if fmname]
    #3. 数据写入tbluserinfo
    sql2=sqldic["update_sql2"]
    cur.execute(sql2)
    usercount=cur.rowcount
    print('###---------￥￥￥￥---insert into tbluserinfo data number: ----',usercount)
    
    #4. 数据写入tblfminfo
    sql3=sqldic["update_sql3"]
    sql3_1=sqldic["update_sql3_1"]
    cur.execute(sql3)
    fmcount=cur.rowcount
    if usercount==fmcount:
        print('###---------￥￥￥￥---insert into tblfminfo data number: ----',fmcount)
    else:
        cur.execute(sql3_1)
        fmcount=cur.rowcount
        print('###---------￥￥￥￥---insert into tblfminfo data number: ----',fmcount)
    if inNum[0]!=usercount:
        cur.execute('SELECT a.fmaddress, b.createtime FROM tmp_create_fminfo a,dbo.tbluserInfo b WHERE a.fmaddress=b.userid')
        existId=cur.fetchall()
        print('\n***',[(i,j.strftime('%Y-%m-%d %H:%M:%S')) for i,j in existId],'this userid has existed in tbluserinfo***')
    if inNum[0]!=fmcount:
        cur.execute('SELECT a.fmaddress, b.createtime FROM tmp_create_fminfo a,dbo.tblfmInfo b WHERE a.fmaddress=b.fmaddress')
        existId=cur.fetchall()
        print('***',[(i,j.strftime('%Y-%m-%d %H:%M:%S')) for i,j in existId],'this userid has existed in tblFMinfo***\n')
    conn.commit()
    #返回2中的查询的userid 连成的字符串
    userid="','".join(usrid) 
    print(userid)
    return usrid,fmname

def check_orgid(usrid,conn,cur):
    """
检验表中两个字段的值是否一致 tblfminfo(orgid) ,tbluserinfo(preid)"""
    if not usrid :
        cur.close()
        conn.close()
        return None
    else:    

        sql=sqldic["check_sql"]
        userid="','".join(usrid) 
        sql2=sqldic["check_sql2"]
        try:
            cur.execute(sql,userid)
            for i in cur:
                if not i[0]:
                    print('this orgid Not Eaqual to preid :',i)  
            cur.execute(sql2,userid)
            for i in cur:
                if (i[1]==i[2] and i[2]==i[3]):
                    print('orgid is ok !')
                else:
                    print(i[0],'--this filiale has different orgid---')
            print('others orgid is ok .')
        except:
            print('----------Check sql error--------------')
        cur.close()
        conn.close()

def host_save(save_id):
    """
将更新过的userid及其对应的serverbase记录在本地数据库"""
    base={'password':'123', 'user':'sa', 'database':'local', 'host':'DESKTOP-K06BHO8'} 
    conn=pymssql.connect(**base)
    cur=conn.cursor()
    cur.executemany("insert into fm2server(userid,filialename,serverbase)values(%s,%s,%s)",save_id)
    print('------total row num:',cur.rowcount,'-------')
    conn.commit()
    cur.close()
    conn.close()
    
 
if __name__=='__main__':
    #程序不需做变更，运行前，整理好excel的serverbase列，将排除其值为#NA的行。
    nowtime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(nowtime)
    data=data_get('E:\\excelfile\\mol.xlsx')
    save_id=[]
    for name,group in data:
        if name!=0:
            print(name,'-----------starting -----')
            conn,cur=insert_to(group,cfg[name])
            userid,uf=update_to(conn,cur)
            check_orgid(userid,conn,cur)
            save_id.extend([(user,filialename,name,) for user,filialename in uf])
            print(name,'------------------Done!-------------------',name,'\n')
    host_save(save_id)
    nowtime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(nowtime,'-----------All-----Done!----------','\n')
   
