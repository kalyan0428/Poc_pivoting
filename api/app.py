import os
import json
import socket
import time

from utility.logger import init_logger, log, metrics_logging
from flask_restplus import reqparse, Api, Resource, fields
import numpy as np

from middleware.filesource import FilesourceMiddleware
from flask import Flask, Response, request
from flask_cors import CORS

import pandas as pd

from etl.database.metadata import Metadata

app = Flask(__name__, static_url_path='/static/')
app.config ['CORS_HEADERS'] ='Content-Type'
Cors = CORS(app)

api = Api(app)
init_logger()

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)


filesource_parser= reqparse.RequestParser()
filesource_parser.add_argument('file_source_type', help='Application id',location='headers', required=False)

#Defining api models from documentation
model_400 = api.model('Errorresponse400', {'message':fields.String, 'errors':fields.Raw})
model_500 = api.model('Errorresponse400', {'message':fields.Integer, 'errors':fields.String})
model_health_200 =api.model('successResponse200',{'success':fields.Boolean,'status':fields.Integer})

log.info("AB-server api started Successfully")

@api.route('/file_source')
@api.expect(filesource_parser)
@api.response(200, 'Successful')
@api.response(400, 'validation Error', model_400)
@api.response(500, 'Internal processing Error', model_500)
class Filesource(Resource):
    def post(self):
        return_status = None
        result = {}
        start =int(round(time.time()*1000))

        try :
            log.info("api Request Initiated")
            fp = FilesourceMiddleware(filesource_parser)
            return_status, result = fp.run(request)
            log.info("__")
        except :
            result ={}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['status'] =0
            result['message'] = 'Internal Error has Occurred while processing the File request'
        finally:
            #resp = Response(json.dumps(result, cls=NpEncoder) ,status = return_status, mimetype ="application/json")
            resp = Response(json.dumps(result) ,status = return_status, mimetype ="application/json")
            #metrics_logging(request, resp, int(round(time.time() * 100 ))start)    
        return resp

@api.route('/workspaces')
@api.response(200, 'Successful')
@api.response(400, 'validation Error', model_400)
@api.response(500, 'Internal processing Error', model_500)
class Workspaces(Resource):
    def get(self):
        return_status = None
        result = {}
        start =int(round(time.time()*1000))        
        try :
            etl_metadata_obj=Metadata()
            id=request.args.get("id")
            data=etl_metadata_obj.select_workflows(id)

            return_status = 200
            result = {"status":"success",
                      "data":data
                       }
            log.info("__")
        except :
            result ={}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['status'] =0
            result['message'] = 'Internal Error has Occurred while processing the File request'
        finally:
            resp = Response(json.dumps(result), status = return_status, mimetype ="application/json") 
            del etl_metadata_obj
        return resp
    def post(self):
        return_status = None
        result = {}
        start =int(round(time.time()*1000))        
        try :
            etl_metadata_obj=Metadata()
            data=etl_metadata_obj.insert(request.json)
            return_status = 200
            result = {"status":"success",
                      "data":data
                       }
            log.info("__")
        except :
            result ={}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['status'] =0
            result['message'] = 'Internal Error has Occurred while processing the File request'
        finally:
            resp = Response(json.dumps(result), status = return_status, mimetype ="application/json") 
            del etl_metadata_obj
        return resp

@api.route('/download')
@api.response(200, 'Successful')
@api.response(400, 'validation Error', model_400)
@api.response(500, 'Internal processing Error', model_500)
class Downloadfiles(Resource):
    def post(self):
        return_status = None
        result = {}
        start =int(round(time.time()*1000))        
        try :
            file_name=""
            file_name=request.json["file_name"]
            columns=request.json["columns"]
            if file_name:
              download_file=file_name.split('\\')[-1]
            
            download_file=r"C:\\Users\\venkats_mandadapu\\{}".format(download_file)
            chunk_size=1000000
            reader = pd.read_csv(file_name, header=0, iterator=True)
            chunks = []
            loop = True
            while loop:
                try:
                   chunk = reader.get_chunk(chunk_size)[columns]
                   chunks.append(chunk)
                except StopIteration:
                    loop = False
                    print("Iteration is stopped")

            df_ac = pd.concat(chunks, ignore_index=True)
            df_ac.to_csv(download_file, index=False)
            return_status = 200
            result = {"status":"success",
                      "download_file":download_file
                       }
            log.info("__")
        except :
            result ={}
            log.exception('Exception while submitting file processing Request')
            return_status = 500
            result['status'] =0
            result['message'] = 'Internal Error has Occurred while processing the File request'
        finally:
            resp = Response(json.dumps(result), status = return_status, mimetype ="application/json") 
        return resp


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    log.info(port)
    log.info("runing ...")
    app.run(host = '127.0.0.1', port=port)