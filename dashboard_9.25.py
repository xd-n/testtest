#!/usr/bin/env python
# coding: utf-8

# In[75]:


import pandas as pd
import numpy as np
import glob
import os

from itertools import groupby
import flatbread as fb
import re


# In[76]:


#added 9.14
import warnings
warnings.filterwarnings("ignore")


# In[77]:


#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)


# In[78]:


pp="/Users/apple/Desktop/Dashboard_0922"
#pp="/Users/kerryyang/Desktop/Dashboard_0919"


# In[79]:


pmap=pd.read_excel(pp+'/SKU_LOB/GDV_Forecast_Conversion_9.19.xlsx', sheet_name="Sheet1")


# In[80]:


def f_get_sortedlist(dir = None):
    Filelist = []
    file_list = os.listdir(dir)
    #sorted_list = sorted(file_list) # 正序
    sorted_list = sorted(file_list,reverse=True) # 逆序

    for s in sorted_list:
        if not s == '.DS_Store' and not s.startswith('~$'): # mac文件夹中的隐藏文件
            file_dir = os.path.join(dir,s)
            Filelist.append(file_dir)

    return Filelist


# In[81]:


def f_create_new_t2_data(filename = None):
    filedata = pd.read_excel(filename,header = 1,index_col = False)
    
    name = os.path.split(filename)
    name = name[-1]
    name = name.split('.')
    name = name[0]
    
    newdata = filedata.iloc[:,0:5]

    newdata.columns=['Apple ID','MPN','Sales','Return','Inventory']
    newdata = newdata.groupby(['MPN'], as_index=False).sum()
    
    output = {'filename':name, 'data':newdata}
    
    return output


# In[82]:


#f_create_new_disti_1230755_inv_data
#f_create_new_disti_1600856_inv_data

def f_create_new_disti_inv_data(filename = None, n="zarva"):
    if n=="zarva":
        filedata = pd.read_csv(filename, delim_whitespace=True, skiprows=0,names=('Date','Apple ID','MPN',
                                                                              'Inventory','Other'), 
                                                                                error_bad_lines=False)
    elif n=="shim":
        filedata = pd.read_table(filename, delim_whitespace=True, skiprows=2, names=('Other', 'MPN', 'Inventory'))
        
    name = os.path.split(filename)
    name = name[-1]
    name = name.split('.')
    name = name[0][0:7]

    newdata = filedata.copy()
   
    if n=="zarva":
        newdata = newdata[['Apple ID', 'MPN', 'Inventory']]
    elif n=="shim":
        newdata.insert(0, 'Apple ID', '1600856')
        newdata = newdata[['Apple ID', 'MPN', 'Inventory']]
    
    newdata = newdata.groupby(['Apple ID', 'MPN'], as_index=False)['Inventory'].sum()
    
    output = {'filename':name, 'data':newdata}
    
    return output

#9.13 changed read table to read csv


# In[83]:


#trf
def f_create_new_disti_1230755_trf_data(filename = None):
    filedata = pd.read_csv(filename, delim_whitespace=True, names=('Date','Apple ID','Reseller ID','Name',
                                                                   'Country','Other','Date2','MPN','Sales',
                                                                   'Other2','Other3','Currency'),encoding='gb2312', error_bad_lines=False)
    
    newdata = filedata.copy()
    newdata=newdata[['Apple ID', 'Reseller ID', 'MPN', 'Sales']]
    newdata = newdata.groupby(['Reseller ID','MPN'], as_index=False)['Sales'].sum()
    
    Distributor_TRF_reseller_ID = newdata['Reseller ID'].unique()
    
    Distributor_TRF = {}

    for i,iReseller in enumerate(Distributor_TRF_reseller_ID):
        Distributor_TRF[i] = Distributor_get_reseller_ID_data(data = newdata,reseller_ID = iReseller)

    return Distributor_TRF


def f_create_new_disti_1600856_trf_data(filename = None):
    filedata = pd.read_table(filename, delim_whitespace=True, skiprows=2,
                   names=('Other', 'MPN', 'Sales','Return','Reseller ID',"G"))
  
    newdata = filedata.copy()
    newdata=newdata[['Reseller ID', 'MPN', 'Sales']]
    newdata.insert(0, 'Apple ID', '1600856')
    newdata = newdata.groupby(['Reseller ID','MPN'], as_index=False)['Sales'].sum()
    compare = pd.read_excel(pp+'/仓位Map/SN仓位-HQID对照表.xlsx')[['Apple ID','HQID']]
    a = pd.merge(newdata, compare, left_on='Reseller ID', right_on='Apple ID', how='left')
    a = a[['HQID', 'MPN', 'Sales']]
    newdata = a.rename(columns = {'HQID':'Reseller ID'})

    
    Distributor_TRF_reseller_ID = newdata['Reseller ID'].unique()
    
    Distributor_TRF = {}

    for i,iReseller in enumerate(Distributor_TRF_reseller_ID):
        Distributor_TRF[i] = Distributor_get_reseller_ID_data(data = newdata,reseller_ID = iReseller)

    return Distributor_TRF


# In[84]:


#all trf

#f_create_new_disti_1230755_trf_all_data
#f_create_new_disti_1600856_trf_all_data
#n = 'zarva', shim

def f_create_new_disti_trf_all_data(filename = None, n="zarva"):
    if n=="zarva":
        filedata = pd.read_csv(filename, delim_whitespace=True, names=('Date','Apple ID','Reseller ID',
                                                                   'Name','Country','Other',
                                                                   'Date2','MPN','Sales',
                                                                   'Other2','Other3','Currency'),
                                                                    encoding='gb2312',error_bad_lines=False)
    elif n=="shim":
        filedata = pd.read_csv(filename, delim_whitespace=True, skiprows=2,names=('Other', 'MPN', 'Sales',
                                                                                  'Return','Reseller ID',"G"),
                                                                                   error_bad_lines=False)
        
    name = os.path.split(filename)
    name = name[-1]
    name = name.split('.')
    name = name[0][0:-4]

    newdata = filedata.copy()
    
    if n=='zarva':
        newdata=newdata[['Apple ID', 'Reseller ID', 'MPN', 'Sales']]
    elif n=='shim':        
        newdata=newdata[['Reseller ID', 'MPN', 'Sales']]
        newdata.insert(0, 'Apple ID', '1600856')   
    
    newdata = newdata.groupby(['Reseller ID','MPN'], as_index=False)['Sales'].sum()
    
    output = {'filename':name, 'data':newdata}
    
    return output


# In[85]:



def get_reseller_ID_data(data = None,reseller_ID = None):
    reseller_data = data.loc[data['Apple HQ ID'] == reseller_ID]
    reseller_data.reset_index(drop=True,inplace=True)
    reseller_EOH = pd.DataFrame(reseller_data, columns = ['Apple HQ ID',
                                                          'Marketing Part Number (MPN)',
                                                          'Marketing Part Name','EOH'])
    data = {'reseller_ID':reseller_ID,'reseller_EOH':reseller_EOH}
    
    return data

def get_acv_data(data = None,reseller_ID = None, col = None):
    reseller_data = data.loc[data['HQ ID'] == reseller_ID]
    reseller_data.reset_index(drop=True,inplace=True)
    reseller_EOH = pd.DataFrame(reseller_data, columns = ['HQ ID','MPN','MPN Name'])
    reseller_EOH.insert(3,'差异 (ACV INV)',reseller_data.iloc[:,(col - 1)])
    
    data = {'reseller_ID':reseller_ID,'reseller_ACV':reseller_EOH}
    
    return data

def Distributor_get_reseller_ID_data(data = None,reseller_ID = None):
    reseller_data = data.loc[data['Reseller ID'] == reseller_ID]
    reseller_data.reset_index(drop=True,inplace=True)
    reseller_TRF = pd.DataFrame(reseller_data, columns = ['Reseller ID', 'MPN', 'Sales'])
    TRF_data = {'reseller_ID':reseller_ID, 'reseller_TRF':reseller_TRF}
    
    return TRF_data


# In[86]:


#was 
#create_reseller_new_table_sku
#create_reseller_new_table_category
#create_distributor_new_table_sku
#create_distributor_new_table_category

#sku and category
def f_create_disti_t2_new_table(input_file = None, t='sku'):       
    new_table = pd.DataFrame()
    new_table.insert(0, 'Marketing Part Number (MPN)',input_file['Marketing Part Number (MPN)'])
    if t=='sku':
        new_table.insert(1, 'Subclass',input_file['Subclass'])
    elif t=='category':
        new_table.insert(1, 'Category',input_file['Category'])
    
    return new_table


# In[87]:


#t2 subclass, category 
#get_reseller_subclass_information
#get_reseller_category_information

def f_get_t2_info(new_table = None, acv = None, reseller_sales_inv = None, reseller_eoh = None,
                             distributor_inv = None, distributor_trf = None, tp ='Subclass'):
    
    temp_1 = pd.merge(new_table,
                      reseller_sales_inv,
                      left_on = 'Marketing Part Number (MPN)',
                      right_on = 'MPN',
                      how = 'left')  
    
    temp_2 = pd.merge(temp_1,
                      reseller_eoh,
                      left_on = 'Marketing Part Number (MPN)',
                      right_on = 'Marketing Part Number (MPN)', 
                      how = 'left')
    
    temp_3 = pd.merge(temp_2,
                      distributor_trf,
                      left_on = 'Marketing Part Number (MPN)',
                      right_on = 'MPN',
                      how = 'left')

    temp_4 = pd.merge(temp_3,
                      distributor_inv,
                      left_on = 'Marketing Part Number (MPN)',
                      right_on = 'MPN',
                      how = 'left')   

    temp_4 = temp_4[['Marketing Part Number (MPN)', tp ,'Sales_x','Return','Inventory_x','EOH','Sales_y']]
    temp_4.fillna(0, inplace = True)
    temp_4['ST'] = temp_4['Sales_x'] - temp_4['Return']
    temp_4.rename(columns = {'Inventory_x':'Inv'}, inplace = True)
    temp_4.rename(columns = {'EOH':'BOH'}, inplace = True)
    temp_4.rename(columns = {'Sales_y':'Xfero'}, inplace = True)
    temp_4['EOH'] = temp_4['BOH'] + temp_4['Xfero'] - temp_4['ST']
    temp_4['ACV(EOH-Inv)'] = temp_4['EOH'] - temp_4['Inv']
    order = ['Marketing Part Number (MPN)', tp, 'ST', 'Inv', 'BOH', 'EOH', 'Xfero', 'ACV(EOH-Inv)']
    temp_4 = temp_4[order]
    
    temp_5 = pd.merge(temp_4,
                      acv,
                      left_on = 'Marketing Part Number (MPN)',
                      right_on = 'MPN',
                      how = 'left')

    order = ['Marketing Part Number (MPN)', tp, 'ST', 'Inv', 'BOH', 'EOH', 'Xfero', 'ACV(EOH-Inv)', '差异 (ACV INV)']
    temp_5 = temp_5[order]
    temp_5.fillna(0, inplace = True)

    return temp_5


# In[88]:


#disti  tp - Subclass, Category
#get_distributor_subclass_information
#get_distributor_category_information

def f_get_disti_info(new_table = None, acv = None,
                                distributor_billing = None, distributor_eoh = None,
                                distributor_inv = None, distributor_trf = None, tp ='Subclass'):

    temp_1 = pd.merge(new_table,
                      distributor_eoh,
                      left_on = 'Marketing Part Number (MPN)',
                      right_on = 'Marketing Part Number (MPN)',
                      how = 'left')
    temp_2 = pd.merge(temp_1,
                      distributor_trf,
                      left_on = 'Marketing Part Number (MPN)',
                      right_on = 'MPN',
                      how = 'left')
    temp_3 = pd.merge(temp_2,
                      distributor_billing,
                      left_on = 'Marketing Part Number (MPN)',
                      right_on = 'Apple 部件号',
                      how = 'left')
    temp_3 = temp_3.drop_duplicates(subset = ['Marketing Part Number (MPN)'], keep = 'first')
    temp_3 = temp_3.reset_index(drop = False)

    temp_4 = pd.merge(temp_3,
                      distributor_inv,
                      left_on = 'Marketing Part Number (MPN)',
                      right_on = 'MPN',
                      how = 'left')
    temp_4 = temp_4[['Marketing Part Number (MPN)',tp ,'EOH','上周发货','Inventory','Sales']]
    temp_4.fillna(0, inplace = True)
    temp_4.rename(columns = {'EOH':'BOH','上周发货':'Billing','Sales':'Xfero','Inventory':'Inv'}, inplace = True)
    
    temp_4['EOH'] = temp_4['BOH'] + temp_4['Billing'] - temp_4['Xfero']
    temp_4['ACV(EOH-Inv)'] = temp_4['EOH'] - temp_4['Inv']
    order = ['Marketing Part Number (MPN)', tp, 'BOH', 'Billing', 'Xfero', 'EOH', 'Inv', 'ACV(EOH-Inv)']
    temp_4 = temp_4[order]   
    temp_5 = pd.merge(temp_4,
                      acv,
                      left_on = 'Marketing Part Number (MPN)',
                      right_on = 'MPN',
                      how = 'left')

    order = ['Marketing Part Number (MPN)', tp, 'BOH', 'Billing', 'Xfero', 'EOH', 'Inv', 'ACV(EOH-Inv)', '差异 (ACV INV)']
    temp_5 = temp_5[order]
    temp_5.fillna(0, inplace = True)
        
    return temp_5


# In[89]:



def f_get_t2_trf_from_disti(reseller_ID = None, distributor_trf = None):
    Reseller_TRF_from_Dist = []
    
    Reseller_ID_list = []
    for i in range(0,len(distributor_trf.keys())):
        Reseller_ID_list.append(distributor_trf[i]['reseller_ID'])
    
    if reseller_ID in Reseller_ID_list:
        index = (Reseller_ID_list.index(reseller_ID))
        Reseller_TRF_from_Dist = distributor_trf[index].get('reseller_TRF')
    else:
        Reseller_TRF_from_Dist = pd.DataFrame()
        Reseller_TRF_from_Dist.insert(0,'Reseller ID',0)
        Reseller_TRF_from_Dist.insert(1,'MPN',0)
        Reseller_TRF_from_Dist.insert(2,'Sales',0)
    
    return Reseller_TRF_from_Dist


# In[90]:


#get index - eoh acv , reseller index

def f_get_eoh_index(eoh_ID = None, eoh_file = None):
    EOH_data = []
    
    EOH_ID_list = []
    for i in range(0,len(eoh_file.keys())):
        EOH_ID_list.append(eoh_file[i]['reseller_ID'])
    
    if eoh_ID in EOH_ID_list:
        index = (EOH_ID_list.index(eoh_ID))
        EOH_data = eoh_file[index].get('reseller_EOH')
    else:
        EOH_data = pd.DataFrame()
        EOH_data.insert(0,'Apple HQ ID',0)
        EOH_data.insert(1,'Marketing Part Number (MPN)',0)
        EOH_data.insert(2,'Marketing Part Name',0)
        EOH_data.insert(3,'EOH',0)
    
    return EOH_data


def f_get_acv_index(acv_ID = None, acv_file = None):
    ACV_data = []
    
    ACV_ID_list = []
    for i in range(0,len(acv_file.keys())):
        ACV_ID_list.append(acv_file[i]['reseller_ID'])
    
    if acv_ID in ACV_ID_list:
        index = (ACV_ID_list.index(acv_ID))
        ACV_data = acv_file[index].get('reseller_ACV')
    else:
        ACV_data = pd.DataFrame()
        ACV_data.insert(0,'HQ ID',0)
        ACV_data.insert(1,'MPN',0)
        ACV_data.insert(2,'MPN Name',0)
        ACV_data.insert(3,'差异 (ACV INV)',0)
    
    return ACV_data


def f_get_t2_index(reseller_ID = None, reseller_file = None):
    reseller_data = []
    
    reseller_ID_list = []
    for i in range(0,len(reseller_file.keys())):
        reseller_ID_list.append(reseller_file[i]['filename'])
    
    if reseller_ID in list(map(int,reseller_ID_list)):
        index = (list(map(int,reseller_ID_list)).index(reseller_ID))
        reseller_data = reseller_file[index].get('data')
    else:
        reseller_data = pd.DataFrame()
        reseller_data.insert(0,'MPN',0)
        reseller_data.insert(1,'Apple ID',0)
        reseller_data.insert(2,'Sales',0)
        reseller_data.insert(3,'Return',0)
        reseller_data.insert(4,'Inventory',0)
    
    return reseller_data


def f_get_TS_index(eoh_ID = None, eoh_file = None):
    EOH_data = []
    
    EOH_ID_list = []
    for i in range(0,len(eoh_file.keys())):
        EOH_ID_list.append(eoh_file[i]['reseller_ID'])
    
    if eoh_ID in EOH_ID_list:
        index = (EOH_ID_list.index(eoh_ID))
        EOH_data = eoh_file[index].get('reseller_TS')
    else:
        EOH_data = pd.DataFrame()
        EOH_data.insert(0,'Apple HQ ID',0)
        EOH_data.insert(1,'Marketing Part Name',0)
        EOH_data.insert(2,'Marketing Part Number (MPN)',0)
        EOH_data.insert(3,'EOH',0)
        EOH_data.insert(4,'TS1',0)
        EOH_data.insert(5,'TS2',0)
        EOH_data.insert(6,'TS3',0)
        EOH_data.insert(7,'TS4',0)
 
    return EOH_data


# In[91]:


def reduce_col_space(df=None):
    list1 = df.columns.values.tolist()
    list2 = []
    for i in list1:
        i = ' '.join(i.split())
        if i.startswith('EOH'):
            i = 'EOH'
            list2.append(i)
        else:
            list2.append(i)        
    df.columns = list2
    return df


# In[248]:


###--------1、首先读取 Mapping
#LOB_SKU文件，以获得SKU、LOB和Category之间的关系------------###

#mapping table!

# 新品发布时会有所更新，因此需要存放最新的文件
d_sku_lob = pmap[['FPH1', 'FPH3', 'Sub-class', 'MPN_ID', 'MPN_name']]
d_sku_lob.columns = ['Category', 'FPH Level 3 (Name)', 'Subclass', 
                        'Marketing Part Number (MPN)', 'Marketing Part Name']

#d_sku_lob


# In[93]:


#check MPN unique # 检查是否有重复的MPN，结果是无重复
def check_MPN(file):
    if len(file["Marketing Part Number (MPN)"].unique())-len(file["Marketing Part Number (MPN)"])==0:
        print("MPN unique -- ok")
    else: print("MPN unique -- not ok")
check_MPN(d_sku_lob)


# In[94]:


###-------------2、读取“GDV”中的Distributor和T2_Reseller的EOH-----------------###

#read -- Channel Online GDV Report FY22Q4WK11.xlsx
#was GDV_file and GDV path
#GDV_filedata

d_GDV = pd.read_excel(f_get_sortedlist(pp+'/EOH/')[0], sheet_name = 'GDV')
d_GDV = reduce_col_space(d_GDV)[['HQ_ID', 'MPN_name', 'MPN_ID', 'EOH']]
d_GDV.columns=['Apple HQ ID','Marketing Part Name','Marketing Part Number (MPN)','EOH']
#d_GDV


# In[95]:


#was dict_T2_Reseller_EOH

dict_t2_EOH = {}

GDV_file_reseller_ID = d_GDV['Apple HQ ID'].unique()

for i,iReseller in enumerate(GDV_file_reseller_ID):
    dict_t2_EOH[i] = get_reseller_ID_data(data = d_GDV,reseller_ID = iReseller)

#dict_t2_EOH 


# In[96]:


#added 9.16
#d_GDV.isna().sum()


# In[97]:


#d_GDV=d_GDV.fillna(0) #-- might do not need to fill to 0


# In[98]:


###---------3、从Distributor文件夹中读取Distributor的INV和TRF数据-------------###

# 读取Distributor的所有文件

disti_file_list = f_get_sortedlist(pp+'/Distributor/')
print(disti_file_list)


# In[99]:


def f_disti_inv_trf(a="zarva",t="inv"):
    if a=="zarva":
        n='1230755'
        INV_file = glob.glob(pp+'/Distributor/'+n+'_AP_INV*.txt')        
        TRF_file = glob.glob(pp+'/Distributor/'+n+'_AP_TRF*.txt')
        if t=="inv":
            d = f_create_new_disti_inv_data(INV_file[0], n=a) 
        elif t=="trf":
            d = f_create_new_disti_1230755_trf_data(TRF_file[0])
    elif a=="shim":
        n='1600856'
        INV_file = glob.glob(pp+'/Distributor/'+n+'_AP_INV*.txt')        
        TRF_file = glob.glob(pp+'/Distributor/'+n+'_AP_TRF*.txt')        
        if t=="inv":
            d = f_create_new_disti_inv_data(INV_file[0], n=a) 
        elif t=="trf":
            d = f_create_new_disti_1600856_trf_data(TRF_file[0])

    #return INV_file,TRF_file
    return d
    


# In[101]:


#f_disti_inv_trf(a="zarva",t="trf")


# In[102]:


###--------4、读取T2_Resller所有的信息表（根据sku合并相同的数据行并累加）---------###

# 读取T2_reseller的所有文件

t2_file_list = f_get_sortedlist(pp+'/T2_Reseller/')


# 将T2_reseller的所有数据整理好后存放在T2_Reseller_data中
dict_t2_data = {}

for i,iFile in enumerate(t2_file_list):
    #print(iFile)
    
    dict_t2_data[i] = f_create_new_t2_data(iFile)


# In[103]:


###-------------5、读取“ACV”中的Distributor和T2_Reseller的历史ACV-----------------###

ACV_file_list = f_get_sortedlist(pp+'/ACV/')

print(ACV_file_list)

d_ACV = pd.read_excel(ACV_file_list[0], sheet_name = 'ACV Trend')

cols_number = d_ACV.shape[1]

dict_recent_ACV = {}

Last_ACV_ID = d_ACV['HQ ID'].unique()

for i,iReseller in enumerate(Last_ACV_ID):
    dict_recent_ACV[i] = get_acv_data(data = d_ACV,reseller_ID = iReseller, col = cols_number)

#dict_recent_ACV
#was last ACV


# In[104]:


###---------------6-1、新建表格以存储
#t2 Reseller_1718445的结果数据---------------###

#and t2 Reseller_2081437

#t-- sku, sublob, category

def f_t2_data(n="1679066", t='sku'):
    if n=="1718445" or n=="2081437":
        disti_trf= f_disti_inv_trf(a="zarva",t="trf")
        disti_inv= f_disti_inv_trf(a="zarva",t="inv")
    elif n=="1645634": 
        disti_trf= f_disti_inv_trf(a="shim",t="trf")
        disti_inv= f_disti_inv_trf(a="shim",t="inv")
    elif n=="1679066":
        disti_trf= f_disti_inv_trf(a="shim",t="trf")
        disti_inv= f_disti_inv_trf(a="shim",t="inv")
    elif n=="1633293":
        disti_trf= f_disti_inv_trf(a="shim",t="trf")
        disti_inv= f_disti_inv_trf(a="shim",t="inv")
        
    Reseller = f_get_t2_index(reseller_ID = int(n), reseller_file = dict_t2_data)
    
    Reseller_EOH = f_get_eoh_index(eoh_ID = int(n), eoh_file = dict_t2_EOH)
    
    Dist_Reseller_TRF = f_get_t2_trf_from_disti(reseller_ID = int(n), distributor_trf = disti_trf)
    
    #added 9.23
    if n=='1679066':
        Dist_Reseller_TRF = Dist_Reseller_TRF.groupby('MPN').sum()
    
    Dist_Reseller_INV = disti_inv.get('data')
    
    Reseller_ACV = f_get_acv_index(acv_ID = int(n), acv_file = dict_recent_ACV).groupby(['HQ ID','MPN'], as_index=False).sum()
    
    #for sku       
    new_table_sku = f_create_disti_t2_new_table(input_file = d_sku_lob, t='sku')
    #print(new_table_sku) #--all 421
    sku = f_get_t2_info(new_table = new_table_sku, acv = Reseller_ACV,
                             reseller_sales_inv = Reseller, reseller_eoh = Reseller_EOH,
                             distributor_inv = Dist_Reseller_INV, distributor_trf = Dist_Reseller_TRF, 
                             tp ='Subclass')  
    #print(sku)   #1679066 - 587 ###问题，多了
    
    #for sublob
    sublob=sku.groupby(['Subclass'], as_index=False).sum()
    
    #for category
    new_table_cate = f_create_disti_t2_new_table(input_file = d_sku_lob, t='category')

    category = f_get_t2_info(new_table = new_table_cate, acv = Reseller_ACV,
                             reseller_sales_inv = Reseller, reseller_eoh = Reseller_EOH,
                             distributor_inv = Dist_Reseller_INV, distributor_trf = Dist_Reseller_TRF,
                             tp = 'Category')

    category = category.groupby(['Category'], as_index=False).sum()
         
    
    if t=='sku':
        return sku
    elif t=='sublob':
        return sublob
    elif t=='category':
        return category
    


# In[105]:


###-------------6-2、新建表格以存储
#Distributor_1230755的结果数据--------------###
#zarva

def f_disti_data(n="zarva", t='sku'):
    if n=="zarva":
        name='-Zarva.xlsx'
        m='1230755'
        INV_file = glob.glob(pp+'/Distributor/'+m+'_AP_INV*.txt')        
        TRF_file = glob.glob(pp+'/Distributor/'+m+'_AP_TRF*.txt')
        disti_trf= f_disti_inv_trf(a="zarva",t="trf")
        disti_inv= f_disti_inv_trf(a="zarva",t="inv")
        all_TRF = f_create_new_disti_trf_all_data(TRF_file[0], n=n)
    elif n=="shim":
        name='-SHIM.xlsx'
        m='1600856'
        INV_file = glob.glob(pp+'/Distributor/'+m+'_AP_INV*.txt')        
        TRF_file = glob.glob(pp+'/Distributor/'+m+'_AP_TRF*.txt')
        disti_trf= f_disti_inv_trf(a="shim",t="trf")
        disti_inv= f_disti_inv_trf(a="shim",t="inv")
        all_TRF = f_create_new_disti_trf_all_data(TRF_file[0], n=n)
    
    Distributor_EOH = f_get_eoh_index(eoh_ID = int(m), eoh_file = dict_t2_EOH)    

    Dist_TRF = all_TRF.get('data')
    Dist_INV = disti_inv.get('data')

    #acv
    Dist_Billing = pd.read_excel(pp+'/Allocation_Billing/ShipmentPlan'+name,header = 7)
    if list(Dist_Billing)[0]!='帐户 ID':
            Dist_Billing = pd.read_excel(pp+'/Allocation_Billing/ShipmentPlan'+name,header = 8)
    Dist_Billing = Dist_Billing[['帐户 ID','Apple 部件号','上周发货']]

    Dist_ACV = f_get_acv_index(acv_ID = int(m), acv_file = dict_recent_ACV).groupby(['HQ ID','MPN'], as_index=False).sum()
    
    #sku
    new_table_sku = f_create_disti_t2_new_table(input_file = d_sku_lob, t='sku')

    sku = f_get_disti_info(new_table = new_table_sku, acv = Dist_ACV,
                                            distributor_billing = Dist_Billing, distributor_eoh = Distributor_EOH,
                                            distributor_inv = Dist_INV, distributor_trf = Dist_TRF, tp='Subclass')        
    #sublob
    sublob = sku.groupby(['Subclass'], as_index=False).sum()
    
    #category
    new_table_cate = f_create_disti_t2_new_table(input_file = d_sku_lob, t='category')

    category = f_get_disti_info(new_table = new_table_cate, acv = Dist_ACV,
                                            distributor_billing = Dist_Billing, distributor_eoh = Distributor_EOH,
                                            distributor_inv = Dist_INV, distributor_trf = Dist_TRF, tp='Category')
    category = category.groupby(['Category'], as_index=False).sum()

    if t=='sku':
        return sku
    elif t=='sublob':
        return sublob
    elif t=='category':
        return category


# In[107]:


#f_disti_data()


# In[108]:


#----------------7.1 allocation - zarva  - shipment file 2 excel file


# In[109]:


ship_file_list = f_get_sortedlist(pp+'/Allocation_Billing/')
ship_file_list


# In[116]:


#2 shipment - location 

def f_ship_location(a="zarva"):
    if a=="zarva":
        b='-Zarva.xlsx'        
    elif a=="shim":
        b='-SHIM.xlsx'
    
    allo_data = pd.read_excel(pp+'/Allocation_Billing/ShipmentPlan'+b, header = 5)
    allo_data2 = pd.read_excel(pp+'/Allocation_Billing/ShipmentPlan'+b, header = 4)
       
    location = allo_data.columns.values.tolist()[8] ##FY22 Q4 WK11 old might be wrong
    location2= allo_data2.columns.values.tolist()[8] #9.22 - w13
    print("current week:",location2)    #-- current week: W13
    
    allocation = allo_data.iloc[:, [0, 1, 4, 8, 9, 10]] 
    
    allocation.columns=['帐户 ID', '帐户名称', 'Apple 部件号', '发货计划', '发货计划.1', '发货计划.2']
    return allocation


# In[117]:


shim_allo=f_ship_location("shim")


# In[118]:


zarva_allo=f_ship_location("zarva")


# In[121]:


#f_ship_location()


# In[122]:


#disti
#t-- sku, sublob, category

def f_disti_allocation(n="zarva", t='sku'):
    if n=="zarva":
        m="1230755"
    if n=="shim":
        m="1600856"
    
    table1 = pd.DataFrame() 
    table1.insert(0, 'Marketing Part Number (MPN)',d_sku_lob['Marketing Part Number (MPN)'])
    
    if t=='sku' or t=='sublob':    
        table1.insert(1, 'Subclass',d_sku_lob['Subclass'])
    elif t=='category':
        table1.insert(1, 'Category',d_sku_lob['Category'])
    table1 = pd.merge(table1,
                      f_ship_location(n),  
                      left_on = 'Marketing Part Number (MPN)',
                      right_on = 'Apple 部件号',
                      how = 'left')
    #table1.to_excel("allocation_table1.xlsx")
    table1 = table1.iloc[: , [0, 1, 5, 6, 7]]
    table1.fillna(0, inplace = True)   
    #sku
    if t=='sku':
        sku = table1.copy()
        return sku
    #sublob
    if t=='sublob':
        sublob = table1.groupby(['Subclass'], as_index=False).sum()
        return sublob
    #category  
    if t=='category':
        category = table1.groupby(['Category'], as_index=False).sum()
        return category


# In[124]:


#f_disti_allocation(n="shim", t='sublob')


# In[64]:


#-------------7.2 allocation - zarva

#Shim not sure?


# In[125]:


#check
test=pd.read_excel(pp+'/Allocation_Billing/ShipmentPlan-SHIM.xlsx', header = 5)
print(test.columns.values.tolist()[8])


# In[126]:


#SN
for i in [0, 1, 4, 9, 14, 18]:
    print(test.columns[i])


# In[131]:


#shim temp


# In[132]:


##### need to confirm col and row location

def f_shim_location_temp(t2="SN"):
    #SHIM发货1679066-SN
    allocation_data = pd.read_excel(pp+'/Allocation_Billing/ShipmentPlan-SHIM.xlsx', header = 5)
    a1 = allocation_data.columns.values.tolist()[8]
    if not a1.startswith('FY22'):
        allocation_data = pd.read_excel(pp+'/Allocation_Billing/ShipmentPlan-SHIM.xlsx', header = 4)
    
    if t2=="SN":
        m='1679066-SN'
        if 'WK12' in allocation_data.columns.values.tolist()[8]:
            allocation = allocation_data.iloc[:, [0, 1, 4, 9, 13, 18]]
        elif 'WK13' in allocation_data.columns.values.tolist()[8]:
            allocation = allocation_data.iloc[:, [0, 1, 4, 9, 14, 18]]
        else:
            allocation = allocation_data.iloc[:, [0, 1, 4, 9, 13, 17]]
    elif t2=="GM":
        m='1645634-GM'
        if 'WK12' in allocation_data.columns.values.tolist()[8]:
            allocation = allocation_data.iloc[:, [0, 1, 4, 10, 14, 19]]
        elif 'WK13' in allocation_data.columns.values.tolist()[8]:
            allocation = allocation_data.iloc[:, [0, 1, 4, 10, 15, 19]]
        else:
             allocation = allocation_data.iloc[:, [0, 1, 4, 10, 14, 18]]
    elif t2=="QT":
        m='1633293-QT'
        if 'WK12' in allocation_data.columns.values.tolist()[8]:
            allocation = allocation_data.iloc[:, [0, 1, 4, 11, 15, 20]]
        elif 'WK13' in allocation_data.columns.values.tolist()[8]:
            allocation = allocation_data.iloc[:, [0, 1, 4, 11, 16, 20]]
        else:
            allocation = allocation_data.iloc[:, [0, 1, 4, 11, 15, 19]]
    
    allocation.columns=['帐户 ID', '帐户名称', 'Apple 部件号', m, m+'.1', m+'.2']   
    return allocation
    


# In[133]:


#f_shim_location_temp(t2='QT')


# In[134]:


###shim temp
def f_shim_allocation_temp(t2="SN", t='category'):
    if t2=="SN":
        m='1679066-SN'
    elif t2=="GM":
        m='1645634-GM'
    elif t2=="QT":
        m='1633293-QT'
        
    new_table = pd.DataFrame()
    new_table.insert(0, 'Marketing Part Number (MPN)', d_sku_lob['Marketing Part Number (MPN)'])
                
    if t=='category':
        new_table.insert(1, 'Category', d_sku_lob['Category'])
        x='Category'
    elif t=='sku' or t=='sublob':
        new_table.insert(1, 'Subclass', d_sku_lob['Subclass'])
        x='Subclass'
       
    new_table = pd.merge(new_table,
                        f_shim_location_temp(t2), #from previous function
                        left_on = 'Marketing Part Number (MPN)',
                        right_on = 'Apple 部件号',
                        how = 'left')
    
    new_table = new_table[['Marketing Part Number (MPN)', x , m, m+'.1', m+'.2']]
    new_table.fillna(0, inplace = True)
    
    if t=='sku':
        sku_allo = new_table.copy()
        return sku_allo
    elif t=='sublob':
        sublob_allo = new_table.groupby(['Subclass'], as_index=False).sum()
        return sublob_allo
    if t=='category':
        category_allo = new_table.groupby(['Category'], as_index=False).sum()
        return category_allo


# In[136]:


#f_shim_allocation_temp(t2="QT",t='sublob')


# In[137]:


#------------------8 TAI --concat categoty and so on....


# In[138]:


#9.23 - 9.24
#zarva
#concat_category----------------------- 1718445 and 2081437  


# In[141]:


#TAI t= sku, sublob, category
def f_TAI_data(t='sku'):
    if t=='sku':
        mn1='Marketing Part Number (MPN)'
        num, num1,num2=23,6,16
        l1=[0,10,11,12]
    elif t=='sublob':
        mn1='Subclass'
        num,num1,num2=21,5,14
        l1=[0,9,10,11]
    elif t=='category':
        mn1='Category'
        num,num1,num2=21,5,14
        l1=[0,9,10,11]

    #t2
    MN=f_t2_data(n='1718445',t=t)+f_t2_data(n='2081437',t=t)
    MN[mn1]=f_t2_data(n='1718445',t=t)[mn1]
    MN=MN.copy()
    MN.insert(0,'T2_Reseller','1718445-MN')

    SN=f_t2_data(n='1679066',t=t)
    SN.insert(0,'T2_Reseller','1679066-SN')

    GM=f_t2_data(n='1645634',t=t)
    GM.insert(0,'T2_Reseller','1645634-GM')

    QT=f_t2_data(n='1633293',t=t)
    QT.insert(0,'T2_Reseller','1633293-QT')

    t2=pd.concat([MN,SN,GM,QT],axis = 0)
        
    
    #disti   
    z = f_disti_data("zarva", t=t)
    z.insert(l1[0],'Distributor','1230755-Zarva')
    z.insert(l1[1],'发货计划',f_disti_allocation(n="zarva", t=t)['发货计划'])
    z.insert(l1[2],'发货计划+1',f_disti_allocation(n="zarva", t=t)['发货计划.1'])
    z.insert(l1[3],'发货计划+2',f_disti_allocation(n="zarva", t=t)['发货计划.2'])

    f0 = f_disti_data("shim", t=t)
    f0.insert(0,'Distributor','1600856-SHIM')
    f0.insert(9,'发货计划',f_shim_allocation_temp(t2='SN', t=t)['1679066-SN'])
    f0.insert(10,'发货计划+1',f_shim_allocation_temp(t2='SN', t=t)['1679066-SN.1'])
    f0.insert(11,'发货计划+2',f_shim_allocation_temp(t2='SN', t=t)['1679066-SN.2'])

    f1 = f_disti_data("shim", t=t)
    f1.insert(0,'Distributor','1600856-SHIM')
    f1.insert(9,'发货计划',f_shim_allocation_temp(t2='GM', t=t)['1645634-GM'])
    f1.insert(10,'发货计划+1',f_shim_allocation_temp(t2='GM', t=t)['1645634-GM.1'])
    f1.insert(11,'发货计划+2',f_shim_allocation_temp(t2='GM', t=t)['1645634-GM.2'])

    f2 = f_disti_data("shim", t=t)
    f2.insert(0,'Distributor','1600856-SHIM')
    f2.insert(9,'发货计划',f_shim_allocation_temp(t2='QT', t=t)['1633293-QT'])
    f2.insert(10,'发货计划+1',f_shim_allocation_temp(t2='QT', t=t)['1633293-QT.1'])
    f2.insert(11,'发货计划+2',f_shim_allocation_temp(t2='QT', t=t)['1633293-QT.2'])

    disti = pd.concat([z,f0,f1,f2],axis = 0)

    d_TAI_one = pd.concat([t2,disti],axis = 1)


    d_TAI_one.insert(num,'TAI',0)

    for i in range(0,len(d_TAI_one)):
        d_TAI_one.iloc[i,num] = d_TAI_one.iloc[i,num1] + d_TAI_one.iloc[i,num2] + d_TAI_one.iloc[i,num-3]
        + d_TAI_one.iloc[i,num-2] + d_TAI_one.iloc[i,num-1] #21, 23       

    d_TAI_one.to_excel(pp+'/Result_new/TAI_'+ t +'.xlsx')
    
    return d_TAI_one


# In[247]:


#f_TAI_data(t='sublob')


# In[773]:


#----end TAI-----


# In[384]:


#-----9. 获取Week数并提取GDV------


# In[154]:


#SHIM shipment

#locate to get week

ship_SHIM = pd.read_excel(pp+'/Allocation_Billing/ShipmentPlan-SHIM.xlsx', header = 5)


# In[155]:


ship_SHIM.columns


# In[156]:


#loaction the 8th - 9.19 FY22 Q4 WK11 固定第九列 [8]

myString = ship_SHIM.columns.values.tolist()[8]
myString
#9.19 changed a to myString


# In[157]:



if not myString.startswith('FY22'):
    ship_SHIM = pd.read_excel(pp+'/Allocation_Billing/ShipmentPlan-SHIM.xlsx', header = 4)
ship_SHIM.columns


# In[389]:


#myString = allocation_data.columns.values.tolist()[8]
#myString


# In[158]:


myWeek = myString.split()[2]
print(myWeek)
newWeek = [''.join(list(g)) for k, g in groupby(myWeek, key=lambda x: x.isdigit())]


# In[159]:


#check shipmentplan zarva
ship_ZARVA=pd.read_excel(pp+'/Allocation_Billing/ShipmentPlan-ZARVA.xlsx', header = 5)
ship_ZARVA.columns


# In[392]:


#d - st week name


# In[160]:


#new 9.19
vars = ["d1", "d2", "d3", "d4", "d5"]
for i in range(len(vars)):
    globals()[vars[i]] = 'ST W' + str(int(newWeek[1]) - (i-1))
    print(vars[i],":", globals()[vars[i]])  
    
#c[1]-- from file name


# In[199]:


newWeek[0],newWeek[1],myWeek


# In[163]:


vars = ["e1", "e2", "e3", "e4", "e5","e6", "e7", "e8", "e9", "e10"]
for i in range(len(vars)):
    globals()[vars[i]] = 'Fcst W' + str(int(newWeek[1]) + (i-1))
    print(vars[i],":", globals()[vars[i]])
    
#old e1- 'Fcst W10', e10 - 'Fcst W19'


# In[164]:


#GDV again

GDV_file_list = f_get_sortedlist(pp+'/EOH/')
print(GDV_file_list)


# In[165]:


newWeek[1],newWeek[0]


# In[166]:


#col names , gdv file data
#list1=[f1,f2,f3,f4,f5,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10]
if newWeek[1] == '1':
    GDV_filedata3 = pd.read_excel(GDV_file_list[0], sheet_name = 'GDV')   
    GDV_filedata3 = reduce_col_space(GDV_filedata3)[['HQ_ID','MPN_name','MPN_ID','ST W9','ST W10','ST W11','ST W12']]
    f1 = 'ST W13'
    f2 = 'ST W12'
    f3 = 'ST W11'
    f4 = 'ST W10'
    f5 = 'ST W9' 
    e1 = 'Fcst W13'
    e2 = 'Fcst W1 new' 
    e3 = 'Fcst W2 new'
    e4 = 'Fcst W3 new' 
    e5 = 'Fcst W4 new' 
    e6 = 'Fcst W5 new' 
    e7 = 'Fcst W6 new' 
    e8 = 'Fcst W7 new' 
    e9 = 'Fcst W8 new' 
    e10 = 'Fcst W9 new'
elif newWeek[1] == '2':
    GDV_filedata3 = pd.read_excel(GDV_file_list[0], sheet_name = 'GDV')
    GDV_filedata3 = reduce_col_space(GDV_filedata3)[['HQ_ID','MPN_name','MPN_ID','ST W10','ST W11','ST W12','ST W13']]
    f1 = 'ST W1'
    f2 = 'ST W13'
    f3 = 'ST W12'
    f4 = 'ST W11'
    f5 = 'ST W10' 
    e1 = 'Fcst W1'
    e2 = 'Fcst W2' 
    e3 = 'Fcst W3'
    e4 = 'Fcst W4'
    e5 = 'Fcst W5'
    e6 = 'Fcst W6'
    e7 = 'Fcst W7'
    e8 = 'Fcst W8'
    e9 = 'Fcst W9'
    e10 = 'Fcst W10'
elif newWeek[1] == '3':
    GDV_filedata1 = pd.read_excel(GDV_file_list[0], sheet_name = 'GDV')
    GDV_filedata1 = reduce_col_space(GDV_filedata1)[['HQ_ID','MPN_name','MPN_ID','ST W1']]    
    GDV_filedata2 = pd.read_excel(GDV_file_list[1], sheet_name = 'GDV')
    GDV_filedata2 = reduce_col_space(GDV_filedata2)[['HQ_ID','MPN_name','MPN_ID','ST W11','ST W12', 'ST W13']]    
    GDV_filedata3 = pd.merge(GDV_filedata2, GDV_filedata1, on=['HQ_ID','MPN_name','MPN_ID'], how='outer')
    f1 = 'ST W2'
    f2 = 'ST W1'
    f3 = 'ST W13'
    f4 = 'ST W12'
    f5 = 'ST W11'
    e1 = 'Fcst W2'
    e2 = 'Fcst W3' 
    e3 = 'Fcst W4'
    e4 = 'Fcst W5'
    e5 = 'Fcst W6'
    e6 = 'Fcst W7'
    e7 = 'Fcst W8'
    e8 = 'Fcst W9'
    e9 = 'Fcst W10'
    e10 = 'Fcst W11'
    
    
elif newWeek[1] == '4':
    GDV_filedata1 = pd.read_excel(GDV_file_list[0], sheet_name = 'GDV')
    GDV_filedata1 = reduce_col_space(GDV_filedata1)[['HQ_ID','MPN_name','MPN_ID','ST W1','ST W2']]
    GDV_filedata2 = pd.read_excel(GDV_file_list[1], sheet_name = 'GDV')
    GDV_filedata2 = reduce_col_space(GDV_filedata2)[['HQ_ID','MPN_name','MPN_ID','ST W12', 'ST W13']]
    GDV_filedata3 = pd.merge(GDV_filedata2, GDV_filedata1, on=['HQ_ID','MPN_name','MPN_ID'], how='outer')
    f1 = 'ST W3'
    f2 = 'ST W2'
    f3 = 'ST W1'
    f4 = 'ST W13'
    f5 = 'ST W12'
    e1 = 'Fcst W3'
    e2 = 'Fcst W4' 
    e3 = 'Fcst W5'
    e4 = 'Fcst W6'
    e5 = 'Fcst W7'
    e6 = 'Fcst W8'
    e7 = 'Fcst W9'
    e8 = 'Fcst W10'
    e9 = 'Fcst W11'
    e10 = 'Fcst W12'
    
elif newWeek[1] == '5':
    GDV_filedata1 = pd.read_excel(GDV_file_list[0], sheet_name = 'GDV')    
    GDV_filedata1 = reduce_col_space(GDV_filedata1)[['HQ_ID','MPN_name','MPN_ID','ST W1','ST W2','ST W3']]
    GDV_filedata2 = pd.read_excel(GDV_file_list[1], sheet_name = 'GDV')
    GDV_filedata2 = reduce_col_space(GDV_filedata2)[['HQ_ID','MPN_name','MPN_ID','ST W13']]
    GDV_filedata3 = pd.merge(GDV_filedata2, GDV_filedata1, on=['HQ_ID','MPN_name','MPN_ID'], how='outer')
    f1 = 'ST W4'
    f2 = 'ST W3'
    f3 = 'ST W2'
    f4 = 'ST W1'
    f5 = 'ST W13'
    e1 = 'Fcst W4'
    e2 = 'Fcst W5' 
    e3 = 'Fcst W6'
    e4 = 'Fcst W7'
    e5 = 'Fcst W8'
    e6 = 'Fcst W9'
    e7 = 'Fcst W10'
    e8 = 'Fcst W11'
    e9 = 'Fcst W12'
    e10 = 'Fcst W13'
    
else:
    GDV_filedata3 = pd.read_excel(GDV_file_list[0], sheet_name = 'GDV')
    GDV_filedata3 = reduce_col_space(GDV_filedata3)[['HQ_ID','MPN_name','MPN_ID',d5, d4, d3, d2]]
    f1 = d1
    f2 = d2
    f3 = d3
    f4 = d4
    f5 = d5 
    if newWeek[1] == '13':
        e1 = 'Fcst W12'
        e2 = 'Fcst W13' 
        e3 = 'Fcst W1 new'
        e4 = 'Fcst W2 new'
        e5 = 'Fcst W3 new'
        e6 = 'Fcst W4 new'
        e7 = 'Fcst W5 new'
        e8 = 'Fcst W6 new'
        e9 = 'Fcst W7 new'
        e10 = 'Fcst W8 new'
    elif newWeek[1] == '6':
        e1 = 'Fcst W5'
        e2 = 'Fcst W6' 
        e3 = 'Fcst W7'
        e4 = 'Fcst W8'
        e5 = 'Fcst W9'
        e6 = 'Fcst W10'
        e7 = 'Fcst W11'
        e8 = 'Fcst W12'
        e9 = 'Fcst W13'
        e10 = 'Fcst W1 new'
    elif newWeek[1] == '7':
        e1 = 'Fcst W6'
        e2 = 'Fcst W7' 
        e3 = 'Fcst W8'
        e4 = 'Fcst W9'
        e5 = 'Fcst W10'
        e6 = 'Fcst W11'
        e7 = 'Fcst W12'
        e8 = 'Fcst W13'
        e9 = 'Fcst W1 new'
        e10 = 'Fcst W2 new'
    elif newWeek[1] == '8':
        e1 = 'Fcst W7'
        e2 = 'Fcst W8' 
        e3 = 'Fcst W9'
        e4 = 'Fcst W10'
        e5 = 'Fcst W11'
        e6 = 'Fcst W12'
        e7 = 'Fcst W13'
        e8 = 'Fcst W1 new'
        e9 = 'Fcst W2 new'
        e10 = 'Fcst W3 new'    
    elif newWeek[1] == '9':
        e1 = 'Fcst W8'
        e2 = 'Fcst W9' 
        e3 = 'Fcst W10'
        e4 = 'Fcst W11'
        e5 = 'Fcst W12'
        e6 = 'Fcst W13'
        e7 = 'Fcst W1 new'
        e8 = 'Fcst W2 new'
        e9 = 'Fcst W3 new'
        e10 = 'Fcst W4 new'    
    elif newWeek[1] == '10':
        e1 = 'Fcst W9'
        e2 = 'Fcst W10' 
        e3 = 'Fcst W11'
        e4 = 'Fcst W12'
        e5 = 'Fcst W13'
        e6 = 'Fcst W1 new'
        e7 = 'Fcst W2 new'
        e8 = 'Fcst W3 new'
        e9 = 'Fcst W4 new'
        e10 = 'Fcst W5 new'
    elif newWeek[1] == '11':
        e1 = 'Fcst W10'
        e2 = 'Fcst W11' 
        e3 = 'Fcst W12'
        e4 = 'Fcst W13'
        e5 = 'Fcst W1 new'
        e6 = 'Fcst W2 new'
        e7 = 'Fcst W3 new'
        e8 = 'Fcst W4 new'
        e9 = 'Fcst W5 new'
        e10 = 'Fcst W6 new'
    elif newWeek[1] == '12':
        e1 = 'Fcst W11'
        e2 = 'Fcst W12' 
        e3 = 'Fcst W13'
        e4 = 'Fcst W1 new'
        e5 = 'Fcst W2 new'
        e6 = 'Fcst W3 new'
        e7 = 'Fcst W4 new'
        e8 = 'Fcst W5 new'
        e9 = 'Fcst W6 new'
        e10 = 'Fcst W7 new'  
        


# In[414]:


#needed to fillna or not?
#GDV_filedata3=GDV_filedata3.fillna(0) #added 9.16


# In[167]:


GDV_filedata3.columns=['Apple HQ ID','Marketing Part Name','Marketing Part Number (MPN)','TS1','TS2','TS3','TS4']
GDV_filedata3


# In[175]:


def f_get_t2_ID_TS(data = None,reseller_ID = None):
    reseller_data = data.loc[data['Apple HQ ID'] == reseller_ID]
    reseller_data.reset_index(drop=True,inplace=True)
    reseller_TS = pd.DataFrame(reseller_data, columns = ['Apple HQ ID',
                                                          'Marketing Part Name',
                                                          'Marketing Part Number (MPN)','TS1','TS2','TS3','TS4'])
    data = {'reseller_ID':reseller_ID,
            'reseller_TS':reseller_TS}
    
    return data


# In[178]:


t2_TS_LastFiveWeek = {}

GDV_file_reseller_ID = GDV_filedata3['Apple HQ ID'].unique()

for i,iReseller in enumerate(GDV_file_reseller_ID):
    t2_TS_LastFiveWeek[i] = f_get_t2_ID_TS(data = GDV_filedata3,reseller_ID = iReseller)


# In[200]:


#t2_TS_LastFiveWeek[2]['reseller_TS']


# In[241]:


list_t2=[1718445,1645634,1679066,1633293]
T2_TS=[]
for i in list_t2:
    t2_TS = f_get_TS_index(eoh_ID = i, eoh_file = t2_TS_LastFiveWeek)
    t2_TS.fillna(0, inplace = True)
    T2_TS.append(t2_TS)

t2_all_TS = pd.concat([i for i in T2_TS])
#t2_all_TS


# In[196]:


#GDV 
GDV_filedata4 = pd.read_excel(GDV_file_list[0], sheet_name = 'GDV')
GDV_filedata4 = reduce_col_space(GDV_filedata4)[['HQ_ID', 'MPN_name', 'MPN_ID', 'ST QTD']]
GDV_filedata4.columns=['Apple HQ ID','Marketing Part Name','Marketing Part Number (MPN)','ST QTD']
GDV_filedata4.drop(['Marketing Part Name'], axis=1, inplace=True)
#GDV_filedata4

t2_all_TS2 = pd.merge(t2_all_TS, GDV_filedata4, on=['Apple HQ ID','Marketing Part Number (MPN)'], how='left')


# In[210]:


#9.25
def f_new_TAI_output(t='category'):
    
    dt1 = pd.merge(t2_all_TS2, d_sku_lob, on='Marketing Part Number (MPN)', how='right')
    dt2 = dt1[['Apple HQ ID','Marketing Part Number (MPN)', 'Subclass', 'Category', 'TS1','TS2','TS3','TS4', 'ST QTD']]
    
    if t=='sku':
        new1 = pd.read_excel(pp+'/Result_new/TAI_sku.xlsx')
        dt5 = dt1[['Apple HQ ID', 'Category', 'Subclass', 'Marketing Part Number (MPN)', 'Marketing Part Name_y', 'TS1','TS2','TS3','TS4', 'ST QTD']]
        dt5.rename(columns={'Marketing Part Name_y':'Marketing Part Name'}, inplace=True)
        dt5.dropna(subset=['Apple HQ ID'], inplace=True)
        dt6 = dt5.groupby(['Apple HQ ID','Category', 'Subclass', 'Marketing Part Number (MPN)', 'Marketing Part Name']).sum()
        new2 = new1[['T2_Reseller', 'Marketing Part Number (MPN)', 'Inv', 'Inv.1', 'ACV(EOH-Inv).1', '发货计划', '发货计划+1', '发货计划+2']]
    elif t=='sublob':
        new1 = pd.read_excel(pp+'/Result_new/TAI_sublob.xlsx')
        y='Subclass'
        dt4 = dt2.groupby(['Apple HQ ID','Subclass']).sum()
        new2 = new1[['T2_Reseller', y, 'Inv', 'Inv.1', 'ACV(EOH-Inv).1','发货计划', '发货计划+1', '发货计划+2']]
    elif t=='category':
        new1 = pd.read_excel(pp+'/Result_new/TAI_category.xlsx')
        y='Category'
        dt3 = dt2.groupby(['Apple HQ ID','Category']).sum()
        new2 = new1[['T2_Reseller', y, 'Inv', 'Inv.1', '发货计划', '发货计划+1', '发货计划+2']]        
    
    new3 = new2.copy()
    new3['HQ_ID'] = new2['T2_Reseller'].map(lambda x:x.split('-')[0])
    
    if t=='category':
        new3.rename(columns={'Inv':'T2 Inv', 'Inv.1':'总代 Inv', 'HQ_ID':'Apple HQ ID'}, inplace=True)
        new3 = new3[['Apple HQ ID', y, 'T2 Inv', '总代 Inv', '发货计划', '发货计划+1', '发货计划+2']]
    elif t=='sublob':
        new3.rename(columns={'Inv':'T2 Inv', 'Inv.1':'总代 Inv', 'HQ_ID':'Apple HQ ID', 'ACV(EOH-Inv).1':'总代 ACV'}, inplace=True)
        new3 = new3[['Apple HQ ID', y, 'T2 Inv', '总代 Inv', '总代 ACV', '发货计划', '发货计划+1', '发货计划+2']]
    elif t=='sku':
        new3.rename(columns={'Inv':'T2 Inv', 'Inv.1':'总代 Inv', 'HQ_ID':'Apple HQ ID', 'ACV(EOH-Inv).1':'总代 ACV'}, inplace=True)
        new3 = new3[['Apple HQ ID', 'Marketing Part Number (MPN)', 'T2 Inv', '总代 Inv', '总代 ACV', '发货计划', '发货计划+1', '发货计划+2']]
    
    new3['Apple HQ ID'] = new3['Apple HQ ID'].astype(int)
    d_TAI_new=new3.copy()
    
    if t=='sku':
        output_sku = pd.merge(d_TAI_new, dt6, on=['Apple HQ ID', 'Marketing Part Number (MPN)'], how='outer')
        output_sku.fillna(0, inplace = True)
        output_sku2 = pd.merge(output_sku, d_sku_lob, on=['Marketing Part Number (MPN)'], how='left')
        output_sku2 = output_sku2.groupby(['Apple HQ ID','Category', 'Subclass', 'Marketing Part Number (MPN)', 'Marketing Part Name']).sum()
        return output_sku2
    
    elif t=='sublob':
        output_sublob = pd.merge(d_TAI_new, dt4, on=['Apple HQ ID', y], how='outer')
        output_sublob.fillna(0, inplace = True)
        output_sublob = output_sublob[['Apple HQ ID', y, 'T2 Inv', '总代 Inv', '总代 ACV', '发货计划',
                        '发货计划+1', '发货计划+2', 'TS1', 'TS2','TS3','TS4', 'ST QTD']]
        return output_sublob
    
    elif t=='category':
        output_category = pd.merge(d_TAI_new, dt3, on=['Apple HQ ID', y], how='left')
        return output_category


# In[ ]:





# In[443]:


#--------10. get前一周GDV 报数，从T2 Reseller 里抓


# In[229]:


list_t2=[1718445,1645634,1679066,1633293]
T2_list=[]
T2_list_sku=[]
for i in list_t2:
    t2 = f_get_t2_index(reseller_ID = i, reseller_file = dict_t2_data)
    t2['TS5']=t2['Sales']-t2['Return']    

    T2=pd.merge(t2, d_sku_lob, left_on='MPN', right_on='Marketing Part Number (MPN)', how='right' )
    T2['Apple HQ ID'] = i
    T2_new=T2[['Apple HQ ID', 'Subclass', 'TS5']].fillna(0) #was LSTS3
    T2_sku=T2[['Apple HQ ID', 'Marketing Part Number (MPN)', 'TS5']].fillna(0) #was LSTSSKU

    T2_list.append(T2_new)
    T2_list_sku.append(T2_sku)
    


# In[246]:


Reseller_all_LSTS = pd.concat([i for i in T2_list])
Reseller_all_LSTS2 = Reseller_all_LSTS.groupby(['Apple HQ ID', 'Subclass']).sum()
#Reseller_all_LSTS2


# In[245]:


Reseller_all_LSTSSKU = pd.concat([i for i in T2_list_sku])
Reseller_all_LSTSSKU2 = Reseller_all_LSTSSKU.groupby(['Apple HQ ID', 'Marketing Part Number (MPN)']).sum()
#Reseller_all_LSTSSKU2


# In[243]:


output_sublob2 = pd.merge(f_new_TAI_output(t='sublob'), Reseller_all_LSTS2,
                          on=(['Apple HQ ID', 'Subclass']), how='outer').fillna(0)
#output_sublob2


# In[244]:


fcst_sku = pd.merge(f_new_TAI_output(t='sku'), Reseller_all_LSTSSKU2, 
                    on=(['Apple HQ ID', 'Marketing Part Number (MPN)']), how='outer').fillna(0)
#fcst_sku


# In[458]:


#-------11. forecast new--------


# In[259]:


forecast_file = f_get_sortedlist(pp+'/forecast/')
print(forecast_file)

#check columns 9.19
d_fcst=pd.read_excel(pp+'/forecast/fcst_sublob_submit.xlsx') #previous wk


# In[260]:


d_fcst.columns, len(d_fcst.columns)
#'Fcst W10', 'Fcst W11' missing

#was 35


# In[263]:


#in result new
d_r=pd.read_excel(pp+'/Result_new/fcst_sublob_submit.xlsx')
print(d_r.columns)
print(len(d_r.columns))


# In[264]:


forecast = pd.read_excel(forecast_file[0], index_col=[0, 1, 2])
forecast = reduce_col_space(forecast)
forecast.reset_index(inplace=True)
forecast.fillna(0, inplace=True)
forecast


# In[467]:


#-------forecast new End--------


# In[ ]:





# In[257]:


forecast=forecast.rename(columns={'HQ ID':'Apple HQ ID', 'Sub-class':'Subclass'})


# In[270]:


#forecast = forecast.reindex(columns=columns)
forecast[[e1, e2, e3, e4, e5, e6, e7, e8, e9, e10]] = forecast[[e1, e2, e3, e4, e5, e6, e7, e8, e9, e10]].round(0).astype(int)
fcst1=forecast[['Apple HQ ID','Category','Subclass',e1,e2,e3]]

fcst2 = fcst1.groupby(['Apple HQ ID', 'Category']).sum()
fcst2

#id, category, 3 weeks

fcst3 = fcst1.groupby(['Apple HQ ID', 'Subclass']).sum()
#fcst3

fcst4 =forecast[['Apple HQ ID','Category','Subclass',e1, e2, e3, e4, e5, e6, e7, e8, e9, e10]]
fcst5 = fcst4.groupby(['Apple HQ ID', 'Category']).sum()
#fcst5

fcst6 = fcst4.groupby(['Apple HQ ID', 'Subclass']).sum()
fcst6


# In[271]:


x2 = []
for x1 in list(forecast):
    if x1.startswith('Fcst'):
        x2.append(x1)
del(x2[0])
x2


# In[282]:


sublob_conversion_one = d_sku_lob[['Category', 'Subclass']].drop_duplicates(subset=['Category', 'Subclass'])

sublob_conversion_two = d_sku_lob[['Category', 'Subclass', 'Marketing Part Number (MPN)', 'Marketing Part Name']]
sublob_conversion_two


# In[ ]:





# In[281]:


fcst_sublob=pd.merge(output_sublob2, fcst3, on=['Apple HQ ID','Subclass'],how='outer')
fcst_sublob.fillna(0, inplace = True)

fcst_sublob2 = pd.merge(fcst_sublob, sublob_conversion_one, on='Subclass', how='left')
fcst_sublob3 = fcst_sublob2[['Apple HQ ID','Category','Subclass','T2 Inv','总代 Inv','总代 ACV','发货计划','发货计划+1','发货计划+2','TS1','TS2','TS3','TS4','TS5','ST QTD',e1,e2,e3]].dropna(subset=["Subclass"])
fcst_sublob4 = fcst_sublob3.sort_values(by=['Apple HQ ID','Category'], ascending=False)
fcst_sublob4.rename(columns={'TS1':f5, 'TS2':f4, 'TS3':f3, 'TS4':f2, 'TS5':f1}, inplace=True)
fcst_sublob5 = fcst_sublob4.groupby(['Apple HQ ID', 'Category', 'Subclass']).sum()
fcst_sublob5


# In[288]:


fcst_sublob_Demand = pd.merge(output_sublob2, fcst6, on=['Apple HQ ID','Subclass'],how='outer')
fcst_sublob_Demand.fillna(0, inplace = True)
fcst_sublob_Demand2 = pd.merge(fcst_sublob_Demand, sublob_conversion_one, on='Subclass', how='left')
fcst_sublob_Demand2


# In[ ]:





# In[291]:


x=e5.split()[1]+' End WOI'
fcst_sublob_Demand3 = fcst_sublob_Demand2[['Apple HQ ID','Category','Subclass','T2 Inv','总代 Inv','总代 ACV','发货计划','发货计划+1','发货计划+2','TS1','TS2','TS3','TS4','TS5','ST QTD',e1,e2,e3, 
                                          e4, e5, e6, e7, e8, e9, e10]].dropna(subset=["Subclass"])
fcst_sublob_Demand4 = fcst_sublob_Demand3.sort_values(by=['Apple HQ ID','Category'], ascending=False)
# fcst_sublob4.drop(labels='FPH1',axis=1, inplace=True)
fcst_sublob_Demand4.rename(columns={'TS1':f5, 'TS2':f4, 'TS3':f3, 'TS4':f2, 'TS5':f1}, inplace=True)
fcst_sublob_Demand4[x] = (fcst_sublob_Demand4.iloc[:,3:9].sum(axis=1) - 
                                                 fcst_sublob_Demand4.iloc[:,16:20].sum(axis=1))/(fcst_sublob_Demand4.iloc[:,20:25].sum(axis=1)/5)
fcst_sublob_Demand4['Demand'] = (4-fcst_sublob_Demand4[x])*(fcst_sublob_Demand4.iloc[:,20:25].sum(axis=1)/5)
fcst_sublob_Demand4['Demand'] = np.where(fcst_sublob_Demand4['Demand'] < 0, 0, fcst_sublob_Demand4['Demand'])

fcst_sublob_Demand5 = fcst_sublob_Demand4.groupby(['Apple HQ ID', 'Category', 'Subclass']).sum()
fcst_sublob_Demand5 = fcst_sublob_Demand5.round({x:1})
fcst_sublob_Demand5['Demand'] = fcst_sublob_Demand5['Demand'].astype(int)
fcst_sublob_Demand6 = fcst_sublob_Demand5.replace(np.inf, 0)
# fcst_sublob_Demand6.to_excel('../Result_new/fcst_sublob_Demand.xlsx')
fcst_sublob_Demand6


# In[292]:


#week name f
f5


# In[486]:


fcst_sublob_Demand3.columns #then wk changed


# In[487]:


fcst_sublob_Demand4.columns


# In[488]:


fcst_sublob_Demand5.columns


# In[ ]:





# In[293]:


x5 = []
for x4 in list(fcst_sublob_Demand5):
    if x4.startswith('Fcst'):
        x5.append(x4)
x5


# In[294]:


c=[x6 for x6 in x2 if x6 in x5]
# x7 =[y for y in (x2+x5) if y not in c]
x7 =[y for y in (x2) if y not in c]
x7


# In[298]:


x3 = ['Apple HQ ID','Category','Subclass'] + x7


# In[299]:


fcstrepair = forecast[x3].copy()
fcstrepair.loc[:,x7] = fcstrepair.loc[:,x7].round(0).astype(int)


# In[300]:


fcst7 = fcstrepair.groupby(['Apple HQ ID', 'Subclass']).sum()
fcst7


# In[301]:


fcst7.columns #for new


# In[302]:


fcst_sublob_Demand_all = pd.merge(fcst_sublob_Demand4, fcst7, on=['Apple HQ ID','Subclass'],how='left')


# In[303]:


fcst_sublob_Demand5 = fcst_sublob_Demand_all.groupby(['Apple HQ ID', 'Category', 'Subclass']).sum()
fcst_sublob_Demand5 = fcst_sublob_Demand5.round({x:1})
fcst_sublob_Demand5['Demand'] = fcst_sublob_Demand5['Demand'].astype(int)
fcst_sublob_Demand6 = fcst_sublob_Demand5.replace(np.inf, 0)
fcst_sublob_Demand6 = fcst_sublob_Demand6.replace(-np.inf, 0)


# In[499]:


#------调整列顺序---


# In[304]:


new_cols = [col for col in fcst_sublob_Demand6.columns if col != x ] + [x]
new_cols = [col for col in new_cols if col != 'Demand' ] + ['Demand']


# In[305]:


fcst_sublob_Demand6.columns


# In[306]:


fcst_sublob_Demand6[new_cols].to_excel(pp+'/Result_new/fcst_sublob_Demand.xlsx')

###final sublob demand###########


# In[307]:


#------12. 调整行顺序-- for submitting


# In[308]:


fcst_sublob_Demand7 = fcst_sublob_Demand6[new_cols].reset_index()


# In[309]:


#######sorter############

sorter = ['AirPods 2nd','AirPods 3rd','AirPods 3rd lighting','AirPods Max','AirPods Pro','AirPods Pro 2nd',
          'iPad (9th Gen)','iPad Air 4','iPad Air 5',
          'iPad Mini 6','iPad Pro 11in (3rd Gen)','iPad Pro 13in (5th Gen)',
          'iPhone 12','iPhone 12 mini','iPhone 13','iPhone 13 mini','iPhone 13 Pro','iPhone 13 Pro Max',
          'iPhone 14','iPhone 14 Plus','iPhone 14 Pro',
          'iPhone 14 Pro Max','iPhone SE (3rd Gen)','iMac','Mac Mini','Mac Studio',
          'MacBook Air','MacBook Pro','SE cell','SE Cell 2nd','SE GPS','SE GPS 2nd',
          'Series 3 GPS','Series 7 cell','Series 7 GPS','Series 8 Cell','Series 8 GPS','Ultra']
sorterIndex = dict(zip(sorter, range(len(sorter))))

#####9.15 changed SE cell, Series 7 cell ---C was upper
### mapping table  is lower case-- need change


# In[348]:


#sorterIndex, len(sorterIndex)


# In[314]:


#test
s = 'ABC'
print(re.search('abc', s,re.IGNORECASE))


# In[315]:


###sorter
fcst_sublob_submit = fcst_sublob_Demand7[fcst_sublob_Demand7['Subclass'].isin(sorter)]
fcst_sublob_submit


# In[316]:


#full
#fcst_sublob_Demand7['Subclass'].value_counts()


# In[318]:


#####check sorterindex #######


fcst_sublob_submit['Subclass_Rank'] = fcst_sublob_submit['Subclass'].map(sorterIndex)
fcst_sublob_submit


# In[319]:


#fcst_sublob_submit.Subclass.value_counts()


# In[320]:


fcst_sublob_submit.sort_values(['Apple HQ ID', 'Subclass_Rank'],
        ascending = [True, True], inplace = True)
fcst_sublob_submit2 = fcst_sublob_submit.drop('Subclass_Rank', axis=1)
fcst_sublob_submit2


# In[321]:


#fcst_sublob_submit2.Subclass.value_counts()


# In[322]:


fcst_sublob_submit3 = fcst_sublob_submit2.groupby(['Apple HQ ID', 'Category', 
                                                   'Subclass'], sort=False).sum()
fcst_sublob_submit3


# In[324]:


#fcst_sublob_submit3.columns


# In[325]:


#save to excel -- fcst_sublob_submit.xlsx

fcst_sublob_submit3.to_excel(pp+'/Result_new/fcst_sublob_submit.xlsx')

#####submit##############


# In[326]:


fcst_sublob_submit4 = fcst_sublob_submit2.groupby(['Category', 'Apple HQ ID', 'Subclass']).sum().sort_index(level=0, ascending=False)


# In[327]:


fcst_sublob_submit4.pipe(fb.totals.add, axis=0, level=[2]).to_excel(
    pp+'/Result_new/review.xlsx')


# In[328]:


#----------


# In[329]:


fcst_category = fcst_sublob4.groupby(['Apple HQ ID', 'Category']).sum()
fcst_category.to_excel(pp+'/Result_new/fcst_category.xlsx')


# In[330]:


fcst_sku.reset_index(inplace=True)


# In[331]:


fcst_sku2 = pd.merge(fcst_sku, sublob_conversion_two, on='Marketing Part Number (MPN)', how='right')
#fcst_sku2


# In[333]:


# Accessories = ['MLWK3CH/A','MME73CH/A','MWP22CH/A','MV7N2CH/A','MRXJ2CH/A','MK0C2CH/A','MU8F2CH/A','MXQT2CH/A','MXQU2CH/A','MXNK2CH/A','MXNL2CH/A','MX3L2CH/A','MXLY2FE/A','MMX62FE/A','MU7U2CH/A','MGN03CH/A','MY1W2CH/A','MHJ83CH/A','MD819FE/A','MD826FE/A','MUF82FE/A','MHXH3CH/A','MLA02CH/A','MRME2CH/A','MMTN2FE/A','MX532CH/A','MX542CH/A']
Accessories = d_sku_lob[d_sku_lob['Subclass']=='24Accessories']['Marketing Part Number (MPN)'].tolist()


# In[334]:


Accessories1 = pd.DataFrame(Accessories)
Accessories1.columns=['Marketing Part Number (MPN)']


# In[335]:


fcst_sku3 = fcst_sku2[['Apple HQ ID','Category','Subclass', 'Marketing Part Number (MPN)', 'Marketing Part Name', 'T2 Inv','总代 Inv','总代 ACV','发货计划','发货计划+1','发货计划+2','TS1','TS2','TS3','TS4','TS5', 'ST QTD']]
fcst_sku4 = fcst_sku3.sort_values(by=['Apple HQ ID','Category'], ascending=False)
# fcst_sublob4.drop(labels='FPH1',axis=1, inplace=True)
fcst_sku4.rename(columns={'TS1':f5, 'TS2':f4, 'TS3':f3, 'TS4':f2, 'TS5':f1}, inplace=True)


# In[336]:


fcst_sku5 = pd.merge(fcst_sku4, Accessories1, on='Marketing Part Number (MPN)', 
                     how='right').drop(['Category', 'Subclass'], axis=1)

fcst_sku5.groupby(['Apple HQ ID', 'Marketing Part Number (MPN)', 
                   'Marketing Part Name']).sum().to_excel(pp+'/Result_new/Accessoriessku.xlsx')


# In[337]:


delTAI_category = pd.read_excel(pp+'/Result_new/TAI_category.xlsx', index_col=0)
delTAI_sublob = pd.read_excel(pp+'/Result_new/TAI_sublob.xlsx', index_col=0)
delTAI_sku = pd.read_excel(pp+'/Result_new/TAI_sku.xlsx', index_col=0)


# In[338]:


columnsnames = ['T2_Reseller','Category','ST','Inv','BOH','EOH','Xfero',
                'ACV(EOH-Inv)','差异 (ACV INV)','Distributor','Category',
                'BOH','Billing','Xfero','EOH','Inv','ACV(EOH-Inv)']


# In[339]:


def dells5(df):
    df.drop(['差异 (ACV INV).1', '发货计划', '发货计划+1', '发货计划+2', 'TAI'],
            axis=1,  inplace=True)
    return df


# In[340]:


delTAI_category1 = dells5(delTAI_category)


# In[341]:


delTAI_sublob1 = dells5(delTAI_sublob)


# In[342]:


delTAI_sku1 = dells5(delTAI_sku)


# In[343]:


delTAI_category1.columns = ['T2_Reseller','Category','ST','Inv','BOH',
                            'EOH','Xfero','ACV(EOH-Inv)','差异 (ACV INV)',
                            'Distributor','Category','BOH','Billing','Xfero',
                            'EOH','Inv','ACV(EOH-Inv)']


# In[344]:


delTAI_sublob1.columns = ['T2_Reseller','Subclass','ST','Inv',
                          'BOH','EOH','Xfero','ACV(EOH-Inv)','差异 (ACV INV)',
                          'Distributor','Subclass','BOH','Billing','Xfero','EOH',
                          'Inv','ACV(EOH-Inv)']


# In[345]:


delTAI_sku1.columns = ['T2_Reseller','Marketing Part Number (MPN)', 'Subclass',
                       'ST','Inv','BOH','EOH','Xfero','ACV(EOH-Inv)',
                       '差异 (ACV INV)','Distributor','Marketing Part Number (MPN)',
                       'Subclass2','BOH','Billing','Xfero','EOH','Inv','ACV(EOH-Inv)']

#changed 9.19 Subclass2


# In[346]:


delTAI_category1.to_excel(pp+'/Result_new/TAI_category.xlsx')


# In[347]:


delTAI_sublob1.to_excel(pp+'/Result_new/TAI_sublob.xlsx')
delTAI_sku1.to_excel(pp+'/Result_new/TAI_sku.xlsx')


# In[544]:


# do not remove for now 9.13

#os.remove('../Result_new/TAI_sku.xlsx')
#os.remove('../Result_new/TAI_category.xlsx')
#os.remove('../Result_new/fcst_category.xlsx')
#os.remove('../Result_new/fcst_sublob_Demand.xlsx')


# In[546]:


print("done!")


# In[ ]:




