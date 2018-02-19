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
from base4.msconfig import cfg
from base4.sqlconfig import config

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
    sql='truncate table tmp_create_fminfo'
    addmeter='alter table tmp_create_fminfo add MeterDiameter varchar(30),MeterGG varchar(30)'
    creat='''create table tmp_create_fminfo
    (
   simid varchar(20),
   fmaddress varchar(20),
   usertype  varchar(20),
   bore  varchar(20),
   createtime DATETIME,
   filialename varchar(100),
   MeterDiameter varchar(30),
   MeterGG varchar(30))'''
    try:
        cur.execute(sql)
    except:
        cur.execute(creat)
    columns=[]
    if data.shape==(11,):
        meter=re.match(r'(MAG\d{6,7})[（(](\d{2,3})mm[）)]',data[7])
        sql2='''insert into tmp_create_fminfo(simid , fmaddress , usertype  , bore,MeterGG,MeterDiameter, createtime, filialename)
        values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}')-- [手机号码],[用户编号],[用户类型],[仪表类型],[装卡日期],[filialename]
    '''.format(data[5],data[2],data[1],data[7],meter.group(1),meter.group(2),data[9],data[0])
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

            sql2='''insert into tmp_create_fminfo(simid , fmaddress , usertype, bore,MeterGG,MeterDiameter, createtime, filialename)
            values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}')-- [手机号码],[用户编号],[用户类型],[仪表类型],[装卡日期],[filialename]-- ,[手机编码]
        '''.format(num[5],num[2],num[1],num[7],meter.group(1),meter.group(2),num[9],num[0])
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
    sql1=''' select count(1) from  tmp_create_fminfo'''
    cur.execute(sql1)
    inNum=cur.fetchone()
    print('tmp data number: ----',inNum[0])
    #2.查询将被写入tbluserinfo中的表编码、filialename
    sql2_1='''SELECT a.fmaddress, b.FilialeName FROM tmp_create_fminfo a,dbo.tblFilialeInfo b WHERE a.filialename=b.FilialeName
    AND a.fmaddress NOT IN (SELECT userid FROM tbluserinfo)'''
    cur.execute(sql2_1)
    fmname=cur.fetchall()
    filiale=set([i[1] for i in fmname if fmname])
    print('###---------￥￥￥￥---distinnct filialename is :',filiale)
    usrid=[i[0] for i in fmname if fmname]
    #3. 数据写入tbluserinfo
    sql2='''INSERT INTO tbluserinfo(Officerid,ModifyTime,UserType,userid,username,UserAddress,PrecinctID,FilialeID,InstallDate,AccountsID,cityid,CityName,PrecinctName,FilialeName)
    SELECT b.OfficerID,CONVERT(VARCHAR(16),GETDATE(),121),usertype,a.fmaddress,a.fmaddress,a.fmaddress,b.PrecinctID,b.FilialeID,CONVERT(VARCHAR(10),a.createtime,121),b.AccountsID,
    b.CityId,b.CityName,b.PrecinctName,b.FilialeName FROM tmp_create_fminfo a,dbo.tblFilialeInfo b WHERE a.filialename=b.FilialeName
    AND a.fmaddress NOT IN (SELECT userid FROM tbluserinfo) '''
    cur.execute(sql2)
    usercount=cur.rowcount
    print('###---------￥￥￥￥---insert into tbluserinfo data number: ----',usercount)
    
    #4. 数据写入tblfminfo
    sql3='''INSERT INTO tblfminfo(Officerid,userid,username,simid,fmtype,fmaddress,installdate,AccountsID,FilialeID,FilialeName,cityid,CityName,PrecinctID,PrecinctName)
    SELECT b.OfficerID,a.fmaddress,a.fmaddress,a.simid,c.MeterTypeCode,a.fmaddress,CONVERT(VARCHAR(10),a.createtime,121),b.AccountsID,b.FilialeID,b.FilialeName,
    b.cityid,b.CityName,b.PrecinctID,b.PrecinctName
    FROM tmp_create_fminfo a,dbo.tblFilialeInfo b,dbo.tblMeterType c WHERE a.filialename=b.FilialeName AND a.MeterGG=c.MeterGG 
    and (a.MeterDiameter+'mm'=c.MeterDiameter or 'DN'+a.MeterDiameter=c.MeterDiameter)
    and a.simid not in (select simid from tblfminfo)'''
    sql3_1='''INSERT INTO tblfminfo(Officerid,userid,username,simid,fmtype,fmaddress,installdate,AccountsID,FilialeID,FilialeName,cityid,CityName,PrecinctID,PrecinctName)
    SELECT b.OfficerID,a.fmaddress,a.fmaddress,a.simid,c.MeterTypeCode,a.fmaddress,CONVERT(VARCHAR(10),a.createtime,121),b.AccountsID,b.FilialeID,b.FilialeName,
    b.cityid,b.CityName,b.PrecinctID,b.PrecinctName
    FROM tmp_create_fminfo a,dbo.tblFilialeInfo b,dbo.tblMeterType c WHERE a.filialename=b.FilialeName AND c.MeterGG='GPRS大表' 
    and (a.MeterDiameter+'mm'=c.MeterDiameter or 'DN'+a.MeterDiameter=c.MeterDiameter)
    and a.simid not in (select simid from tblfminfo)'''
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

        sql='''select case a.orgid
       when b.preid then 1 else 0 end ,a.orgid ,b.preid ,b.userid from tblfminfo a, tbluserinfo b 
        where a.fmaddress = b.userid  and b.userid in  (%s) '''
        userid="','".join(usrid) 
        sql2='select filialename,max(orgid),middle(orgid),min(orgid) from tblfminfo where fmaddress in (%s) group by filialename'
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
   
