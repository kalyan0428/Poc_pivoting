import requests
import json
import os
from datetime import datetime
from utility.logger import log
import pandas as pd


from etl.files.fileprocess import Fileprocess

class FilesourceMiddleware:
    def __init__ (self, filesource_parser):
        self.args = filesource_parser.parse_args()
    def run(self, request):
        """
        This function stores the user feedback received from Ul server
        """
        return_status = None
        result = {}
        try:
            total_rows=0
            # validate payload
            log.info(request.json)
            
            etl_obj=Fileprocess(request.json)
            
            file_type=etl_obj.validate()
            if not file_type in ("csv","xlsx"):
                raise ValueError (file_type)
            
            if request.args.get("metadata_ind",None)=="yes":
                 data= etl_obj.file_metadata_process(file_type)
            elif request.args.get("pivot_ind",None) == "yes":
                file_name =request.json.get("file_name",None)
                filter_items =request.json.get("filter",None)
                columns =request.json.get("columns",None)
                rows = request.json.get("rows",None)
                values_section = request.json.get("values",None)
                download_ind= request.json.get("download_ind",None)
                data= etl_obj.file_process(file_type,file_name,filter_items,columns,rows,values_section,download_ind)
            else:
                data= etl_obj.sample_data(file_type)
 
            return_status = 200
            result['status'] = "successful"
            result ['data']=data

        except ValueError as e:
            result = {}
            log.exception("Value Exception while submitting feedback")
            result['status'] = "failed"
            return_status = 400
            result ['message'] = e.args[0]

        except:
            result ={}
            log.exception("Exception while submitting feedback")
            return_status =500
            result['status'] ="failed"
            result['message'] ='internal error has occured while processing the request'
        
        return return_status,result