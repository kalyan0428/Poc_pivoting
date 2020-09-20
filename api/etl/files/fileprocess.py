import pandas as pd
import os
from .filevalidate import Filevalidate
import json
from utility.logger import log
import requests
import collections
import numpy as np
from datetime import datetime
class Fileprocess (Filevalidate):

    def __init__ (self,file_details):
        self.file_details = file_details
    
    def get_file_type (self, filename) :
        return os.path.splitext (filename) [1] [1:]
    
    def validate(self):
        err_msg = None
        if not self.file_exists_check(self.file_details['file_name']):
            err_msg = "File is not exists in this path,please give correct file path details"
        elif not self.non_empty_file_check(self.file_details['file_name']):
            err_msg="File is empty,please give correct non empty file"
        elif not self.file_extension_check(self.file_details['file_name']):
            err_msg="Supported file extensions excel,csv only"
        if not err_msg:
            err_msg=os.path.splitext (self.file_details['file_name']) [1] [1:]

        return err_msg

    def file_process (self,file_type,file_name,filter_items,columns,rows,values_section,download_ind=None,chunk_size=2000000):
       try:
        if file_type == "csv" :
            data_dir=r"C:\\\\Users\\\\venkats_mandadapu\\\\v2\\\\data\\\\"
            #data_dir=r"C:\\\\Users\\\\abhi\\\\venkat\\\\data\\\\"
            pkl_file =data_dir+os.path.basename(os.path.splitext(file_name)[0])
            file_exists=os.path.exists(pkl_file+".PKL")
            download_file=None
            if download_ind:
              download_file=pkl_file+"_"+datetime.now().strftime("%Y-%m-%d-%H-%M-%S")+".csv"
              #print(download_file)
            #print(pkl_file)
            #print(file_exists)
            if not file_exists:
                reader = pd.read_csv(self.file_details['file_name'], header=0, iterator=True)
                chunks = []
                total_rows=0
                while True: 
                    try:                
                        chunk = reader.get_chunk(chunk_size)
                        chunks.append(chunk)
                    except:
                        break
            
                DF=pd.concat(chunks, ignore_index=True)
                DF.to_pickle(f"{pkl_file}.pkl")
            else:
                DF=pd.read_pickle(f"{pkl_file}.pkl")
             
            result_dict={"filter":{},
                         "columns": {},
                         "rows": {},
                         "data": []
                         }
            if columns:
                  for col in columns:
                    result_dict["columns"][col]=[str(v) for v in list(DF[col].head(1000).unique())]
                    #result_dict["columns"][col]=[]
            if rows:
                  for col in rows:
                    result_dict["rows"][col]=[str(v) for v in list(DF[col].head(1000).unique())]
            if filter_items:
                filter_columns=[]
                if columns:
                    filter_columns.extend(columns)
                if rows:
                    filter_columns.extend(rows)               
                for col, vals in filter_items.items():
                  if (col not in filter_columns):
                    #print('***************')
                    #print(col,rows,columns)
                    result_dict["filter"][col]=[str(k)  for k in list(DF[col].head(1000).unique())]
                filter_v={k:v for k,v in filter_items.items() if v}
                if filter_v:
                    DF=DF.loc[DF[list(filter_v)].isin(filter_v).all(axis=1), :]
            #print('--------------------')
            #print(DF.shape)
            #print('--------------------')
            # values_section and  rows and columns
            # values_section and  rows 
            # values_section and  columns (not required)
            # rows and columns
            # rows
            # columns
            # values_section
            if (values_section and  rows and columns):
                #pivot_result=DF.pivot_table(values='Units Sold', index=['Region'], columns=['Item Type'], aggfunc='sum', fill_value=0, margins=True, dropna=False, margins_name='GrandTotal')           
                #print(len(values_section),values_section)
                values=values_section
                if len(values_section) == 1:
                  values=values_section[0]

                
                pivot_result=DF.pivot_table(values=values, index=rows, columns=columns, aggfunc='sum', fill_value=0, margins=True, dropna=True, margins_name='GrandTotal')
                pivot_result=pivot_result.reset_index()
                if download_ind:
                  pivot_result.to_csv(download_file)
                  result_dict["data"]=[{"download_file":download_file}]
                else:
                    result_d=[]
                    for d in pivot_result.to_dict(orient="records"):
                       result={}
                       for k,v in d.items():
                          result.update({str(k):v})
                       result_d.append(result)
                    result_dict["data"]=result_d
                    #result_dict["data"]=json.loads(pivot_result.to_json(orient="records"))
            elif (values_section and  rows):
                  tol_cols=rows+values_section
                  pivot_result=DF[tol_cols].groupby(rows).sum().reset_index()
                  if download_ind:
                    pivot_result.to_csv(download_file)
                    result_dict["data"]=[{"download_file":download_file}]
                  else:
                    result_dict["data"]=json.loads(pivot_result.to_json(orient="records"))
            elif ( rows and columns): 
                total_columns=rows+columns
                val_col=None
                for k,v in DF.dtypes.to_dict().items():
                  if k not in total_columns:
                     val_col=k
                     break
                if  val_col:
                   #print('*****************')
                   #print(val_col,rows,columns)
                   pivot_result=DF.pivot_table(values=val_col, index=rows, columns=columns, aggfunc='count', fill_value=0, margins=True, dropna=True, margins_name='GrandTotal')
                   pivot_result=pivot_result.reset_index()
                   #print('***************')
                   for col in pivot_result.columns:
                       #print('&&&&&')
                       #print(col)
                       #pivot_result[col].values[:] = None
                       if col not in rows:
                           pivot_result[col] = None
                   if download_ind:
                      pivot_result.to_csv(download_file)
                      result_dict["data"]=[{"download_file":download_file}]
                   else:
                      result_dict["data"]=json.loads(pivot_result.to_json(orient="records"))
                
            elif ( columns and values_section):
                  tol_cols=columns+values_section
                  #result_dict["data"]=json.loads(DF[tol_cols].groupby(columns).sum().reset_index().to_json(orient="records"))
                  pivot_result=DF[tol_cols].groupby(columns).sum().reset_index()
                  if download_ind:
                    pivot_result.to_csv(download_file)
                    result_dict["data"]=[{"download_file":download_file}]
                  else:
                    result_dict["data"]=json.loads(pivot_result.to_json(orient="records"))
            elif rows:
                for k,v in DF.dtypes.to_dict().items():
                  if k in rows and (v == np.int64 or v == np.float64):
                     DF[k]=DF[k].astype(str)
                if download_ind:
                    DF[~DF[rows].duplicated()][rows].to_csv(download_file)
                    result_dict["data"]=[{"download_file":download_file}]
                else:
                      result_dict["data"]=[{col:k} for col in rows for k in sorted(list(DF[col].head(1000).unique()))]
            elif columns:
                for k,v in DF.dtypes.to_dict().items():
                  if k in columns and (v == np.int64 or v == np.float64):
                     DF[k]=DF[k].astype(str)
                if download_ind:
                    DF[~DF[columns].duplicated()][columns].to_csv(download_file)
                    result_dict["data"]=[{"download_file":download_file}]
                else:
                    result_dict["data"]=[ {k:"" for col in columns for k in sorted(list(DF[col].head(1000).unique()))}]
            elif values_section:
                pivot_result={}
                pivot_result['sum']=DF[values_section].sum()
                pivot_result=pd.DataFrame(pivot_result)
                if download_ind:
                    pivot_result.to_csv(download_file)
                    result_dict["data"]=[{"download_file":download_file}]
                else:
                     result_dict["data"]=json.loads(pivot_result.to_json(orient="records"))
        else:
           if file_type == "csv" :
                data= json.loads(pd.read_csv(self.file_details['file_name'],nrows=50).head(10).to_json (orient="records"))
           else:
                data=json.loads(pd.read_excel(self.file_details['file_name'],nrows=50).head(10).to_json (orient="records"))
           return data
       except Exception as e:
          print("%s  is error"%e)
       finally:
           del DF
           return result_dict
        
    def sample_data(self,file_type):
           if file_type == "csv" :
                data= json.loads(pd.read_csv(self.file_details['file_name'],nrows=50).head(10).to_json (orient="records"))
           else:
                data=json.loads(pd.read_excel(self.file_details['file_name'],nrows=50).head(10).to_json (orient="records"))
           return data
    def file_metadata_process (self,file_type):
        if file_type == "csv":
            DF=pd.read_csv(self.file_details['file_name'],nrows=1).head(1)
        else:
            DF=pd.read_excel(self.file_details['file_name'],nrows=1).head(1)
        res={}
        for k,v in DF.dtypes.to_dict().items():
            if str(v)=="object":
               v="string"
            res[k]=str(v)
        return res

    def non_excel_file_process (self):
        if self.file_details["delimiter_ind"]=="no":
            with open(self.file_details['file']) as f:
                output=f.readlines ()

        else:
            if self.get_file_type(self.file_details['file']) == "csv" :
                output=pd.read_csv(self.file_details['file'],sep=self.file_details['delimiter']).to_dict(orient="records")
            elif self.get_file_type(self.file_details['file']) == "txt" :
                output= pd.read_fwf (self.file_details['file'],colspecs=self.file_details['colspecs']).to_dict (orient="rescords")
            else:
                output="invalid file,please check the file content"
        return output


