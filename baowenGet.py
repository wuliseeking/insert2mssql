#-*- coding:utf-8 -*-
"""
可多进程获取
1.根据表编号获取smi手机号
2.根据手机号获取某段时间的报文
"""
import pymssql
from base4.msconfig import cfg 
import pandas as pd
import re 
import datetime
import time
from multiprocessing import Pool

def fetch_data(idlist,base,day=10):
    """
根据数据库配置和id列表查出相应的报文数据 以(id,df)的二元列表形式返回数据"""
    conn=pymssql.connect(**base)  #连接数据库
    cur=conn.cursor()
    columns=[]
    #查询获取数据库中数据
    for i in idlist:
        noReport=0
        noSim=0
        #根据id查出smi号
        sql1="SELECT simid FROM tblfminfo WHERE fmaddress='{}' ".format(i) #根据表地址查对应电话号码   
        print('-- smi  search is  executed !-- -- ',sql1)
        cur.execute(sql1)
        ad=cur.fetchall()
        if len(ad):
        #根据sim号查出目标报文
            sql2='''SELECT   * FROM tbldatalog_bak
                    WHERE CreateTime>dateadd(day,-{0},getdate())
                    and (SUBSTRING(datalog,27,33) LIKE '{1}%' or 
                    SUBSTRING(datalog,12,33) like '{1}%') '''.format(day,' 3'.join(list(str(ad[0][0]))))  
            sql2_1='''SELECT   * FROM tbldatalog
                    WHERE CreateTime>dateadd(day,-{0},getdate())
                    and (SUBSTRING(datalog,27,33) LIKE '{1}%' or 
                    SUBSTRING(datalog,12,33) like '{1}%') '''.format(day,' 3'.join(list(str(ad[0][0]))))  
            print(sql2)
            try:
                cur.execute(sql2)
            except:
                cur.execute(sql2_1)
            #每段报文以(id,df)格式存入列表
            sd=cur.fetchall()
            if len(sd):
                print('-- --------------------- baowen has  executed !-- -- ')
                columns.append((i,pd.DataFrame(sd,columns=['pid','createtime','datalog','port','dpflag']),))        
            else :
                noReport+=1
                print(i,'----*'*3 ,' Has No Report ','----*'*2,'total :',noReport)
        else:
            noSim+=1
            print(i,'----#'*3,' has no SIM number ','---#'*2,'total :',noSim)
    cur.close()
    conn.close()
    return columns

def excel2str(Excelinfo):
    """
数据源获取,返回id组成的字符串"""
    #1.读取excel中的数据
    data11=pd.read_excel(**Excelinfo)
    print(data11.columns)
    num=data11.loc[0]
    return str(data11['用户编码'] )

def str2list(strn):
    """
将传入的字符串列表化并去重"""
    mol1=re.compile(r'(\d{6,8})')
    idlist=mol1.findall(strn)
    idset=set()
    id_list=[]
    for i in idlist:
        if i not in idset:
            idset.add(i)
            id_list.append(i)
    return id_list

def data2excel(pdColumns):
    '''
将二元列表（id,pd)组成的列表分工作簿写入excel'''
    nowtime=datetime.datetime.now().strftime('%m%d%H%M')
    filename='e:\\excelfile\\'+nowtime+'.xlsx'
    print('filename is : ',filename)
    with pd.ExcelWriter(filename) as writer:
        for i,j in pdColumns:
            print(i)
            j.to_excel(writer,sheet_name=str(i))
    print('Done!')


Excelinfo={
    'io':'E:\\qqfile\\12-15.xlsx','sheetname':'Sheet1','header':0 ,'skiprows':0 ,'parse_cols':'A:K'}
if __name__=='__main__':
    #需改变源id字符串的信息，和报文获取的近几天的天数，默认近10天，args(day=10),userid多则需分清各进程的任务
    
    t1=time.time()
    #原始userid字符串
    id='1820335'
    #id=excel2str(Excelinfo)

    #字符串列表化
    idlist=str2list(id)
    p=Pool(4)
    result=[]
    for i in range(0,len(idlist)//2*2-2,2):
        result.append(apply_async(fetch_data,args=(idlist[i+4:],,cfg['10.0.shctwater'])))
  
    p.close()#关闭进程池，不让新的进程加入
    p.join()#阻塞进程直到子进程完成
    columns=[]
    for i in result:
        columns+=i.get()
    #将数据写入excel
    try:
        data2excel(columns)
    except:
        print('------All NO Report !-------')
    print('Child process end,spend time(minute) : ',round((time.time()-t1)/60,2))
