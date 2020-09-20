from sqlalchemy import create_engine  
from sqlalchemy import Column, String  
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker

from .settings.db_settings import DATABASE


class Metadata():
    def __init__(self):
        self.db = create_engine(DATABASE['db_connection'] )

    def select_workflows(self,id=None):
        result_dict=[]
        with self.db.begin() as conn:
            
            if id:
                select_sql="select * from work_flows where workspace_id = {}".format(id)
                result = conn.execute(select_sql)
            else:
                select_sql="select * from work_flows"
                result = conn.execute("select * from work_flows")
            column_header=['workspace_id','workspace_name','columns', 'filter','values','rows','file_name','data']
            result_dict=[]
            for row in result:
                rd={}
                rd["workspace_id"]=row[0]
                rd["workspace_name"]=row[1]
                rd.update(row[-1])
                result_dict.append(rd)
        return result_dict
 
          
    def insert(self,record):
        workspace_name=record['workspace_name']
        result_dict={"file_name":record['file_name'],
                     "filter":record['filter'],
                     "columns":record['columns'],
                     "rows": record['rows'],
                     "values": record['values']
                     }
        data=str(result_dict)
        data=data.replace("'","\"")
        columns=None
        filter=None
        value=None
        rows=None
        data_source=None
        insert_sql="""
                   INSERT INTO work_flows(workspace_name,columns,filter,value,rows,data_source,data)
                   VALUES('{}','{}','{}','{}','{}','{}','{}')
                   """.format(workspace_name,columns,filter,value,rows,data_source,data)
        with self.db.begin() as conn:
           conn.execute(insert_sql)
        return "workspace created successfully"

    def update(self,record):
        update_sql="""
                   UPDATE work_flows 
                   set  workspace_name=:workspace_name,
                        columns=:columns,
                        filter=:filter,
                        value=:value,
                        rows=:rows,
                        data_source=:data_source
                   WHERE workspace_id=:workspace_id
                   """
        with self.db.begin() as conn:
                conn.execute(update_sql, {'workspace_id':record['workspace_id'],
                                          'workspace_name':record['workspace_name'], 
                                          'columns':record['columns'],
                                          'filter':record['filter'], 
                                          'value':record['value'],
                                          'rows':record['rows'],
                                          'data_source':record['data_source']
                                          })
    def delete(self,record):
        delete_sql="""
                   DELETE FROM work_flows WHERE workspace_id=:workspace_id
                   """

        with self.db.begin() as conn:
                conn.execute(delete_sql, {'workspace_id':record['workspace_id'] })
