from flask import Flask,request,jsonify,send_file,make_response,abort
import json
from data import DataBase
from datetime import datetime
from logging.config import dictConfig
import xlsxwriter
import os
from io import BytesIO
from functools import wraps
import jwt;
from jwt.algorithms import RSAAlgorithm
from configparser import ConfigParser

dictConfig(
  {
    "version": 1,
    'formatters': {'default': {
    'format': '[%(asctime)s] %(levelname)s in %(module)s: %(funcName)s : %(lineno)d : %(message)s',
    }},
    "handlers": {
      "time-rotate": {
        "class": "logging.handlers.TimedRotatingFileHandler",
        "filename": r"Log/estimator_log.log",
        "when": "D",
        "interval": 1,
        "backupCount": 20,
        "formatter": "default",
      },
    },
    "root": {
      "level": "DEBUG",
      "handlers": ["time-rotate"],
    },
  }
)

app = Flask(__name__)

con = DataBase.getConnection()
cur=con.cursor()
created_by = '';
role = '';

def background(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        global created_by
        global role
        try:
            config = ConfigParser()
            config.read('roles_permissions.ini');
            biPermissions = config['BIManager']['biPermissions'];
            biPermissionList = json.loads(biPermissions)
            etlPermissions = config['ETLManager']['etlPermissions'];
            etlPermissionList = json.loads(etlPermissions)
            qapermissions = config['QAManager']['qaPermissions'];
            qaPermissionList = json.loads(qapermissions)
            adminpermissions = config['Admin']['adminPermissions'];
            adminPermissionList = json.loads(adminpermissions)
            adminViewPermissions = config['Admin']['adminViewPermissions'];
            adminViewPermissionList = json.loads(adminViewPermissions)
            IDjwt = request.headers['Authorization'].replace('Bearer ','')
            config.read('key.ini');
            keyPermissions = config['key_json']['keyJsonAccess'];
            keyPermissionList = json.loads(keyPermissions)
            public_key = RSAAlgorithm.from_jwk(keyPermissionList)
            decoded = jwt.decode(IDjwt, public_key, verify=False, audience='api://a5d2e839-f63f-4dea-9750-d6054ee08dd7', algorithms='RS256')   
            created_by = decoded['email']
            role = decoded['roles']
            if callable(f):
                if decoded['scp'] == 'EstimatorProdAPI':
                    if f.__name__== 'get_Permission_List':
                        app.logger.info('get_Permission_List Calling function inside wrapper Successfully Executed')
                        return f(*args, **kwargs)
                    if 'BIManager' in decoded['roles'] and f.__name__ in biPermissionList:                
                        app.logger.info('BIManager Calling function inside wrapper Successfully Executed')
                        return f(*args, **kwargs)
                    elif 'ETLManager' in decoded['roles'] and f.__name__ in etlPermissionList:                
                        app.logger.info('ETLManager Calling function inside wrapper Successfully Executed')
                        return f(*args, **kwargs)
                    elif 'QAManager' in decoded['roles'] and f.__name__ in qaPermissionList:                
                        app.logger.info('QAManager Calling function inside wrapper Successfully Executed')
                        return f(*args, **kwargs)
                    elif 'Admin' in decoded['roles'] and f.__name__ in adminPermissionList:  
                        if f.__name__ in adminViewPermissionList:
                            created_by=''              
                        app.logger.info('Admin Calling function inside wrapper Successfully Executed')
                        return f(*args, **kwargs)

                    else:
                        app.logger.error('Invalid Authentication token!')
                        return {
                        "message": "Invalid Authentication token!",
                        "data": None,
                        "error": "Unauthorized"
                        }, 401
                
                else:
                    app.logger.error('Invalid Scope!')
                    return {
                      "message": "Invalid Scope!",
                      "data": None,
                      "error": "Unauthorized"
                      }, 401
            else:
                app.logger.error('Task must be a callable')    
                raise TypeError('Task must be a callable')
        except Exception as e:  
            app.logger.error('Invalid Authentication token!: %s', str(e))
            return {
                    "message": "Invalid Authentication token!",
                    "data": None,
                    "error": str(e)
                    }, 401        
    return wrapped

@app.route('/Get_Permission_List',methods=['GET'])
@background
def get_Permission_List():
    try:
        app.logger.info('Get_Permission_List Process Starting')
        userId = created_by
        userrole = str(role[0])
        config = ConfigParser()
        config.read('roles_permissions.ini');
        permissions = config.items(userrole)
        jsonPermission = json.loads(permissions[1][1])
        return {
                    "userId": userId,
                    "userrole": userrole,
                    "permissions": jsonPermission
                    }, 200
 
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return { "Message" :"An ERROR occurred in Get_Permission_List Method",
                 "Status"  : 500}


@app.route('/Bi_Est_Getall',methods=['GET'])
@background
def bi_Get_allEst_tables():
    try:
        app.logger.info('BI_Get All Process Starting')
        con = DataBase.getConnection()
        cur = con.cursor()
        if 'Admin' not in role:
            cur.execute("SELECT * FROM bi_estimator WHERE created_by = %s", [created_by])
            cur.fetchall()
            if cur.rowcount==0:
                app.logger.info('Record Not Found for the New user')
                #return jsonify("Record not found"), 404
                return jsonify([])
        cur.execute  (""" SELECT JSON_ARRAYAGG(  
                              JSON_OBJECT(
                              'categoryId',e.category_id,
                              'categoryName',c.category_name,
                              'biEstimatorId', e.BI_estimator_ID,
                              'projectName', e.projectName,
                              'estimatorName', e.estimatorName,
                              'biName', e.BIName,
                              'totalEffortsInPersonHours', e.totalEfforts_inPersonHours,
                              'retestingEfforts', e.retestingEfforts,
                              'totalEffortsInPersonDays', e.totalEfforts_inPersonDays,
                              'createdDate',e.created_date,
                              'updatedDate',e.updated_date,
                              'isActive',e.is_active,
                              'biTaskGroup', 
                          (SELECT JSON_ARRAYAGG(
                               JSON_OBJECT(
                                        'biTaskGroupId', tg.BI_taskGroup_id,
                                        'taskGroupId',tg.taskgroup_id,
                                        'taskGroupName', tg1.taskgroup_name,
                                        'biEstimatorId',tg.BI_estimator_ID,
                                        'createdDate',tg.created_date,
                                        'updatedDate',tg.updated_date,
                                        'isActive',tg.is_active,
                                        'biTasks', 
                                      (SELECT JSON_ARRAYAGG(
                                           JSON_OBJECT(
                                                'biTaskListId', t.bi_tasklist_id, 
                                                'taskListId',t.tasklist_id, 
                                                'biTaskGroupId',t.BI_taskGroup_id,
                                                'taskName', t2.task_name, 
                                                'simple', t.simple, 
                                                'medium', t.medium, 
                                                'complex', t.complex,
                                                'simpleWf', t.simpleWF, 
                                                'mediumWf', t.mediumWF, 
                                                'complexWf', t.complexWF,
                                                'effortDays', t.effort_days, 
                                                'effortHours', t.effort_hours, 
                                                'createdDate',t.created_date,
                                                'updatedDate',t.updated_date,
                                                'isActive',t.is_active
                        )
                   ) FROM bi_tasklist t inner join tbltasklist t2 on t2.tasklist_id = t.tasklist_id  where t.BI_taskGroup_id =tg.BI_taskGroup_id and t.is_active=1 and t2.is_active=1)
              )
         ) FROM bi_taskgroup tg inner join tbltaskgroup tg1 on tg1.taskgroup_id = tg.taskgroup_id WHERE tg.BI_estimator_ID = e.BI_estimator_ID and tg.is_active=1 and tg1.is_active= 1)
        )
        )FROM bi_estimator e  inner join category c on c.category_id = e.category_id WHERE e.is_active=1 and c.is_active=1 AND CASE WHEN %s = '' THEN 1=1 ELSE e.created_by = %s END """,(created_by,created_by,))
        rows = cur.fetchall()
        result_json_str=rows[0][0]
        result_json = json.loads(result_json_str)
        app.logger.info('BI_Get All request received Successfully')
        return jsonify(result_json)
  
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return { "Message" :"An ERROR occurred in BI GETAll Method",
                 "Status"  : 500}

@app.route('/Bi_EstGetByID/<int:BI_estimator_ID>', methods=['GET'])
@background
def bi_Get_ByID_Estimator(BI_estimator_ID):
    try:
        app.logger.info('BI_Get_By_ID Process Starting')
        con = DataBase.getConnection()
        cur = con.cursor()
        if 'Admin' not in role:
            cur.execute("SELECT * FROM bi_estimator WHERE created_by = %s", [created_by])
            cur.fetchall()
            if cur.rowcount==0:
                  app.logger.info('Record Not Found for the New user')
                  #return jsonify("Record not found"), 404 
                  return jsonify([])
        rows = cur.execute("""SELECT JSON_OBJECT(
                                'categoryId',e.category_id,
                                'categoryName',c.category_name,
                                'biEstimatorId', e.BI_estimator_ID,
                                'projectName', e.projectName,
                                'estimatorName', e.estimatorName,
                                'biName', e.BIName,
                                'totalEffortsInPersonHours', e.totalEfforts_inPersonHours,
                                'retestingEfforts', e.retestingEfforts,
                                'totalEffortsInPersonDays', e.totalEfforts_inPersonDays,
                                'createdDate',e.created_date,
                                'updatedDate',e.updated_date,
                                'isActive',e.is_active,
                                'biTaskGroup', 
                                (SELECT JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'biTaskGroupId', tg.BI_taskGroup_id, 
                                        'taskGroupId', tg.taskgroup_id,
                                        'taskGroupName', tg1.taskgroup_name,
                                        'biEstimatorId',tg.BI_estimator_ID,
                                        'createdDate',tg.created_date,
                                        'updatedDate',tg.updated_date,
                                        'isActive',tg.is_active,
                                        'biTasks', 
                                        (SELECT JSON_ARRAYAGG(
                                            JSON_OBJECT(
                                                'biTaskListId', t.bi_tasklist_id, 
                                                'taskListId',t.tasklist_id,
                                                'biTaskGroupId',t.BI_taskGroup_id,
                                                'taskName', t2.task_name, 
                                                'simple', t.simple, 
                                                'medium', t.medium, 
                                                'complex', t.complex,
                                                'simpleWf', t.simpleWF, 
                                                'mediumWf', t.mediumWF, 
                                                'complexWf', t.complexWF,
                                                'effortDays', t.effort_days, 
                                                'effortHours', t.effort_hours, 
                                                'createdDate',t.created_date,
                                                'updatedDate',t.updated_date,
                                                'isActive',t.is_active
                                            )
                                        ) FROM bi_tasklist t inner join tbltasklist t2 on t2.tasklist_id = t.tasklist_id WHERE t.BI_taskGroup_id = tg.BI_taskGroup_id and t.is_active=1 and t2.is_active=1)
                                  )
                                ) FROM bi_taskgroup tg inner join tbltaskgroup tg1 on tg1.taskgroup_id = tg.taskgroup_id WHERE tg.BI_estimator_ID = e.BI_estimator_ID and tg.is_active=1 and tg1.is_active= 1)
                            ) FROM bi_estimator e  inner join category c on c.category_id = e.category_id WHERE e.BI_estimator_ID = %s and e.is_active=1 and c.is_active=1 AND CASE WHEN %s = '' THEN 1=1 ELSE e.created_by = %s END """, (BI_estimator_ID,created_by,created_by,))                     
        rows = cur.fetchall()
        if len(rows) == 0:
            app.logger.info('Record Not Found for this Specific BI_Id')
            return jsonify("please enter a valid BI_estimator_ID")
        con.close()
        result_json_str = rows[0][0]
        result_json = json.loads(result_json_str)
        app.logger.info('Bi_Get_By_ID request received Successfully')
        return jsonify(f"Showing BI_estimator_ID : {BI_estimator_ID}", result_json)
  
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return { "Message" :"An ERROR occurred in BI GET_BY_ID Method",
                 "Status"  : 500}
  
@app.route('/GetAllCategories', methods=['GET'])
def getAllCategories():
    try:
        app.logger.info('getAllCategories Process Starting')
        con = DataBase.getConnection()
        cur = con.cursor()
        cur.execute("""SELECT JSON_ARRAYAGG(
                                JSON_OBJECT(
                                    'categoryId', c.category_id,
                                    'categoryName', c.category_name
                                )
                            )
                            FROM category as c;""")
        rows = cur.fetchall()
        con.close()
        result_json_str = rows[0][0]
        result_json = json.loads(result_json_str)
        app.logger.info('getAllCategories request received successfully')
        return jsonify(result_json)
    
    except Exception as e:
        app.logger.error(f'Error: {str(e)}')
        return {"Message" :"An Error Occured in Getting getAllCategories",
                "Status"  : 500}
    
@app.route('/GetAllTaskListName/<int:category_id>', methods=['GET'])
def getAllTaskListName(category_id):
  try:
    app.logger.info('GetAllTaskListName Process Starting')
    con = DataBase.getConnection()
    cur = con.cursor()
    cur.execute("""SELECT JSON_ARRAYAGG(
                        JSON_OBJECT(
                            'categoryID', C.category_id,
                            'categoryName', C.category_name,
                            'TaskGroup', (
                                SELECT JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'taskGroupID', tbl.taskgroup_id,
                                        'taskGroupName', tbl.taskgroup_name,
                                        'TaskLists', (
                                            SELECT JSON_ARRAYAGG(
                                                JSON_OBJECT(
                                                    'taskID', tlt.tasklist_id,
                                                    'taskListName', tlt.task_name
                                                )
                                            )
                                            FROM tbltasklist AS tlt
                                            WHERE tlt.taskgroup_id = tbl.taskgroup_id and tlt.is_active = 1
                                        )
                                    )
                                )
                                FROM tbltaskgroup AS tbl
                                WHERE tbl.category_id = C.category_id and tbl.is_active = 1
                            )
                        )
                    ) AS json_data
                    FROM category AS C
                    WHERE C.category_id = %s;""",(category_id,))
    rows = cur.fetchall()
    if len(rows) == 0:
        app.logger.info('Record Not Found for this Specific category_id')
        return jsonify("please enter a valid category_id")
    con.close()
    result_json_str = rows[0][0]
    result_json = json.loads(result_json_str)
    app.logger.info('GetAllTaskListName request received Successfully')
    return jsonify(f"Showing category_id : {category_id}", result_json)

  except Exception as e:
    app.logger.error('An error occurred: %s', str(e))
    return {"Message" :"An Error Occured in Getting GetAllTaskListName",
            "Status"  : 500}
  
@app.route('/Bi_GetFilterValues/<int:category_id>', methods=['GET'])
def bi_getFilterValues(category_id):
    try:
        app.logger.info('bi_getFilterValues Process Starting')
        con = DataBase.getConnection()
        cur = con.cursor()
        cur.execute(""" SELECT JSON_ARRAYAGG(
                           JSON_OBJECT(
                               'categoryID', c.category_id,
                               'categoryName', c.category_name,
                               'estimator', (
                                   SELECT JSON_ARRAYAGG(
                                       JSON_OBJECT(
                                           'estimatorID', e.bi_estimator_id,
                                            'estimatorName', e.estimatorName,
                                           'taskgroup', (
                                               SELECT JSON_ARRAYAGG(
                                                   JSON_OBJECT(
                                                       'taskgroupID', tg.bi_taskgroup_id,
                                                       'taskgroupName', t.taskgroup_name
                                                   )
                                               )
                                               FROM bi_taskgroup tg
                                               inner join tbltaskgroup t
                                               on tg.taskgroup_id = t.taskgroup_id
                                               WHERE tg.bi_estimator_id = e.bi_estimator_id
                                               and t.is_active = 1
                                               and tg.is_active = 1
                                           )
                                       )
                                   )
                                FROM bi_estimator e
                                WHERE c.category_id = e.category_id
                                and e.is_active = 1
                               )
                           )
                       ) AS json_data
                FROM category c
                where c.category_id = %s and c.is_active = 1""",(category_id,))
       
        rows = cur.fetchall()
        if len(rows) == 0:
            app.logger.info('Record Not Found for this Specific category_id')
            return jsonify("please enter a valid category_id")
        con.close()
        result_json_str = rows[0][0]
        result_json = json.loads(result_json_str)
        app.logger.info('bi_getFilterValues request received Successfully')
        return jsonify(f"Showing category_id : {category_id}", result_json)

    except Exception as e:
        app.logger.error(f'Error: {str(e)}')
        return {"Message" :"An Error Occured in Getting bi_getFilterValues",
                "Status"  : 500}
    
@app.route('/Get_Bi_Wf_Values/<int:category_id>', methods=['GET'])
def Get_Bi_Wf_Values(category_id):
  try:
    app.logger.info('Get_Bi_Wf_Values Process Starting')
    con = DataBase.getConnection()
    cur = con.cursor()
    cur.execute("""SELECT JSON_ARRAYAGG(
                            JSON_OBJECT('categoryID',C.category_id,
                                        'categoryName',C.category_name,
                                        'WorkFactor',(
                                        SELECT JSON_ARRAYAGG(
												JSON_OBJECT('workFactorId',twf.workfactor_id,
															'simpleWf',twf.simple_WF,
															'mediumWf',twf.medium_WF,
															'complexWf',twf.complex_WF
                                        )
                                    )FROM tblworkfactor AS twf Where twf.category_id = C.category_id
                                    )
							)
                            )as jsondata FROM category C where C.category_id = %s """,(category_id,))
    rows = cur.fetchall()
    if len(rows) == 0:
        app.logger.info('Record Not Found for this Specific category_id')
        return jsonify("please enter a valid category_id")
    con.close()
    result_json_str = rows[0][0]
    result_json = json.loads(result_json_str)
    app.logger.info('Get_Bi_Wf_Values request received Successfully')
    return jsonify(f"Showing category_id : {category_id}", result_json)

  except Exception as e:
    app.logger.error('An error occurred: %s', str(e))
    return {"Message" :"An Error Occured in Getting Get_Bi_Wf_Values",
            "Status"  : 500}

@app.route('/Bi_Estimator_Updt_Delete', methods=['PUT'])
@background
def bi_updateInsert_Estimator():
    try:
        app.logger.info('Bi_Estimator_Updt_Delete Method Starting')
        request1 = request.get_json()
        for lst in request1:
            biEstimatorId = lst.get("biEstimatorId")
            categoryId=lst["categoryId"]
            projectName = lst["projectName"]
            estimatorName = lst["estimatorName"]
            biName = lst["biName"]
            totalEffortsInPersonHours = lst["totalEffortsInPersonHours"]
            retestingEfforts = lst["retestingEfforts"]
            totalEffortsInPersonDays = lst["totalEffortsInPersonDays"]
            updatedDate = datetime.now()
            biTaskGroup = lst["biTaskGroup"]
            isActive=lst["isActive"]
            app.logger.info('Data update request received')
            con = DataBase.getConnection()
            cur = con.cursor()            
            if biEstimatorId is not None and biEstimatorId != "":
                sql = """UPDATE bi_estimator SET projectName=%s, estimatorName=%s, BIName=%s, 
                         totalEfforts_inPersonHours=%s, retestingEfforts=%s, totalEfforts_inPersonDays=%s,
                         updated_date=%s,is_active=%s WHERE  BI_estimator_ID=%s """
                cur.execute(sql, (projectName, estimatorName, biName, totalEffortsInPersonHours,
                                  retestingEfforts, totalEffortsInPersonDays, updatedDate,isActive, biEstimatorId))
                app.logger.info("bi_estimator Data Updated Successfully")
            else:
                sql = """INSERT INTO bi_estimator(category_id,projectName, estimatorName, BIName, 
                         totalEfforts_inPersonHours, retestingEfforts, totalEfforts_inPersonDays,is_active,created_by)
                         VALUES (%s, %s, %s, %s, %s, %s,%s,%s,%s)"""
                cur.execute(sql, (categoryId,projectName, estimatorName, biName, totalEffortsInPersonHours,
                                  retestingEfforts, totalEffortsInPersonDays,isActive,created_by))
                biEstimatorId = cur.lastrowid
                app.logger.info('bi_estimator Data Newly Inserted Successfully By PUT Method')
            for lst in biTaskGroup:
                biTaskGroupId = lst.get("biTaskGroupId")
                if biTaskGroupId is not None and biTaskGroupId != "":
                    cur.execute('UPDATE bi_taskgroup SET taskgroup_id=%s,updated_date=%s,is_active=%s WHERE BI_taskGroup_id=%s',
                                (lst['taskGroupId'],updatedDate,lst['isActive'], biTaskGroupId))
                    app.logger.info("bi_taskgroup  Data Updated Successfully")
                else:
                    cur.execute('INSERT INTO bi_taskgroup(is_active,taskgroup_id, BI_estimator_ID) VALUES (%s, %s,%s)',
                                (lst['isActive'],lst["taskGroupId"], biEstimatorId))
                    biTaskGroupId = cur.lastrowid
                    app.logger.info('bi_taskgroup Data Newly Inserted Successfully By PUT Method')
                for tsklist in lst["biTasks"]:
                    biTaskListId = tsklist.get("biTaskListId")
                    if biTaskListId is not None and biTaskListId != "":
                        updatedEffortDays = tsklist['simple']*tsklist['simpleWf'] + tsklist['medium']*tsklist['mediumWf'] + tsklist['complex']*tsklist['complexWf']
                        updatedEffortHours= updatedEffortDays*8
                        cur.execute('UPDATE bi_tasklist SET tasklist_id=%s, simple=%s, medium=%s, complex=%s, simpleWF=%s, mediumWF=%s, complexWF=%s, effort_days=%s, effort_hours=%s,updated_date=%s,is_active=%s WHERE bi_tasklist_id=%s',
                                    (tsklist['taskListId'], tsklist['simple'], tsklist['medium'], tsklist['complex'], tsklist['simpleWf'], tsklist['mediumWf'], tsklist['complexWf'], updatedEffortDays, updatedEffortHours, updatedDate,tsklist['isActive'], biTaskListId))
                        app.logger.info("bi_tasklist Data Updated Successfully")
                    else:
                        effort_result_days = tsklist['simple']*tsklist['simpleWf'] + tsklist['medium']*tsklist['mediumWf'] + tsklist['complex']*tsklist['complexWf']
                        effort_result_hrs = effort_result_days*8
                        cur.execute('INSERT INTO bi_tasklist(tasklist_id,simple, medium, complex, simpleWF, mediumWF, complexWF, effort_days, effort_hours, is_active,BI_taskGroup_id) '
                                    'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                    (tsklist['taskListId'], tsklist['simple'], tsklist['medium'], tsklist['complex'], tsklist['simpleWf'], tsklist['mediumWf'], tsklist['complexWf'],
                                      effort_result_days, effort_result_hrs,tsklist['isActive'], biTaskGroupId))
                        app.logger.info('bi_tasklist Data Newly Inserted Successfully By PUT Method')
            con.commit()
            con.close()
            app.logger.info('Bi_Estimator_Updt_Delete Process Successfully Executed')
            return {"Message" : "Bi Data Inserted Or Updated Successfully",
                    "Status"  : 200}
        
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return {"Message" : "An ERROR occurred in Bi_Estimator_Updt_Delete Method",
                "Status"  : 500}

@app.route('/Bi_download_excel_api/<int:category_id>/<estimator_ids>',methods=['GET'])
def bi_downloadExcelApi(category_id, estimator_ids):
    try:
        app.logger.info("Starting Bi_download_excel_api function")
        query = bi_generateQuery(category_id, estimator_ids)
        file_path = bi_writeExcelFile(query)
        app.logger.info("Excel File Returning process successfully executed")
        filename = 'Bi_data.xlsx'
        response = make_response(send_file(file_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))
        response.headers['Content-Disposition'] = f"attachment; filename={filename}"
        return response

    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return {"Message" :"An ERROR occurred in Bi_download_excel_api function",
                "Status"  : 500}

def bi_generateQuery(category_id, estimator_ids):
    query = """
        SELECT  c.category_name,es.projectName,es.estimatorName,es.totalEfforts_inPersonHours,es.retestingEfforts,es.totalEfforts_inPersonDays,tg.taskgroup_name,
         tl2.task_name,tl.simple, tl.medium, tl.complex, tl.simpleWF, tl.mediumWF,tl.complexWF, tl.effort_days, tl.effort_hours
        FROM category c
        INNER JOIN tbltaskgroup tg ON c.category_id = tg.category_id
        INNER JOIN bi_estimator es ON es.category_id = c.category_id
        INNER JOIN bi_taskgroup tg1 ON tg1.BI_estimator_ID = es.BI_estimator_ID
        AND tg1.taskgroup_id = tg.taskgroup_id
        INNER JOIN bi_tasklist tl ON tl.BI_taskGroup_id = tg1.BI_taskGroup_id
        INNER JOIN tbltasklist tl2 ON tl2.tasklist_id=tl.tasklist_id
        WHERE c.category_id = {}
        AND c.is_active = 1
        AND tg.is_active = 1
        AND tg1.is_active = 1
        AND tl.is_active = 1
        AND tl2.is_active = 1
        AND es.is_active = 1
        AND es.BI_estimator_ID IN ({})
    """.format(category_id,estimator_ids)
    return query

def bi_writeExcelFile(query):
    try:
        app.logger.info("Updating data to Excel file")
        con = DataBase.getConnection()
        cur = con.cursor()
        # Create a temporary file path
        temp_dir = os.path.join(app.instance_path, 'temp')
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = os.path.join(temp_dir, 'Bi_data.xlsx')
        workbook = xlsxwriter.Workbook(temp_file)
        worksheet = workbook.add_worksheet()
        header_format = workbook.add_format(
            {'bold': True,
             'bg_color': '#F0FFFF',
             'border': 2
            })
        header_format2 = workbook.add_format({'bold': True, 'bg_color': '#EE4B2B','font_color': '#ffffff','border': 2})
        border_format = workbook.add_format({'border': 2,"align": "Left"})
        merge_format = workbook.add_format(
                                      {
                                          "bold": 1,
                                          "border": 1,
                                          "align": "Left",
                                          "valign": "vcenter",
                                      }
                                  )
        # Merge cells for the image
        worksheet.merge_range("A1:J3", "", merge_format)
        worksheet.insert_image("A1",r"Image/emergere-logo.png",{"x_scale": 0.2, "y_scale": 0.2, "x_offset": 320, "y_offset": 10})
        worksheet.set_column(0,1,25)
        cur.execute(query)
        rows = cur.fetchall()
        if not rows:
            app.logger.warning("No rows returned from the query")
            workbook.close()
            return jsonify(message="No data available for the given parameters")
        # Write main headers
        worksheet.write(3, 0, 'Category Name', header_format)
        worksheet.write(4, 0, 'Project Name', header_format)
        worksheet.write(5, 0, 'Estimator Name', header_format)
        worksheet.write(6, 0,  'Total Efforts In Person Hours', header_format)
        worksheet.write(7, 0,  'Retesting Efforts', header_format)
        worksheet.write(8, 0,  'Total Efforts In Person Days', header_format)
        # Write header values
        category_name = rows[0][0]
        project_name = rows[0][1]
        estimator_name = rows[0][2]
        TotalEffortsInPersonHours=rows[0][3]
        RetestingEfforts=rows[0][4]
        TotalEffortsInPersonDays=rows[0][5]
        worksheet.merge_range("B4:J4", category_name, merge_format,)
        worksheet.merge_range("B5:J5", project_name, merge_format)
        worksheet.merge_range("B6:J6", estimator_name, merge_format)
        worksheet.merge_range("B7:J7", TotalEffortsInPersonHours, merge_format)
        worksheet.merge_range("B8:J8", RetestingEfforts, merge_format)
        worksheet.merge_range("B9:J9", TotalEffortsInPersonDays, merge_format)
        #worksheet.merge_range(0,7, c)
        headers=['Task Group Name','Task Name', 'Simple', 'Medium', 'Complex', 'Simple WF', 'Medium WF', 'Complex WF', 'Effort Days', 'Effort Hours',]
        for col, header_text in enumerate(headers):
           worksheet.write(9, col, header_text, header_format2)
        row_num = 10  # Starting row number for data

        for row_data in rows:
            worksheet.write_row(row_num, 0, row_data[6:], border_format)  # Write remaining data rows
            row_num += 1

        workbook.close()
        app.logger.info("Returning the Excel BI_data")
        return temp_file

    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return {"Message" :"An ERROR occurred in bi_writeExcelFile method",
                "Status"  : 500}

#---------------------------------ETL Estimator----------------------------------------------
@app.route('/Etl_Est_Getall',methods=['GET'])
@background
def etl_Get_allEst_tables():
    try:
        app.logger.info('Estimator ETL Get All Process Starting')
        con = DataBase.getConnection()
        cur = con.cursor()
        if 'Admin' not in role:
            cur.execute("SELECT * FROM etl_estimator WHERE created_by = %s", [created_by])
            cur.fetchall()
            if cur.rowcount==0:
                  app.logger.info('Record Not Found for the Specific ETL ID')
                  #return jsonify("Record not found"), 404 
                  return jsonify([])
        cur.execute  (""" SELECT JSON_ARRAYAGG(  
                              JSON_OBJECT(
                              'categoryId',e.category_id,
                              'categoryName',c.category_name,
                              'etlEstimatorId', e.etl_estimator_ID,
                              'projectName', e.projectName,
                              'estimatorName', e.estimatorName,
                              'etlName', e.etlName,
                              'totalEffortsInPersonHours', e.totalEfforts_inPersonHours,
                              'retestingEfforts', e.retestingEfforts,
                              'totalEffortsInPersonDays', e.totalEfforts_inPersonDays,
                              'createdDate',e.created_date,
                              'updatedDate',e.updated_date,
                              'isActive',e.is_active,
                              'etlTaskGroups', 
                          (SELECT JSON_ARRAYAGG(
                               JSON_OBJECT(
                                        'etlTaskGroupId', tg.etl_taskGroup_id,
                                        'taskGroupId',tg.taskgroup_id,
                                        'taskGroupName', tg1.taskgroup_name,
                                        'etlEstimatorId',tg.etl_estimator_ID,
                                        'createdDate',tg.created_date,
                                        'updatedDate',tg.updated_date,
                                        'isActive',tg.is_active,
                                        'etlTaskLists', 
                                      (SELECT JSON_ARRAYAGG(
                                           JSON_OBJECT(
                                                'etlTaskListId', t.etl_tasklist_id, 
                                                'taskListId',t.tasklist_id,
                                                'etlTaskGroupId',t.etl_taskGroup_id,
                                                'taskName', t2.task_name, 
                                                'simple', t.simple, 
                                                'medium', t.medium, 
                                                'complex', t.complex,
                                                'simpleWf', t.simpleWF, 
                                                'mediumWf', t.mediumWF, 
                                                'complexWf', t.complexWF,
                                                'effortDays', t.effort_days, 
                                                'effortHours', t.effort_hours, 
                                                'createdDate',t.created_date,
                                                'updatedDate',t.updated_date,
                                                'isActive',t.is_active
                        )
                   ) FROM etl_tasklist t inner join tbltasklist t2 on t2.tasklist_id = t.tasklist_id  where t.etl_taskGroup_id =tg.etl_taskGroup_id and t.is_active=1 and t2.is_active=1)
              )
         ) FROM etl_taskgroup tg inner join tbltaskgroup tg1 on tg1.taskgroup_id = tg.taskgroup_id WHERE tg.etl_estimator_ID = e.etl_estimator_ID and tg.is_active=1 and tg1.is_active= 1)
        )
        )FROM etl_estimator e  inner join category c on c.category_id = e.category_id WHERE e.is_active=1 and c.is_active=1 AND CASE WHEN %s = '' THEN 1=1 ELSE e.created_by = %s END """,(created_by,created_by,))
        rows = cur.fetchall()
        result_json_str=rows[0][0]
        result_json = json.loads(result_json_str)
        app.logger.info('ETL Get All Datas request received Successfully')
        return jsonify(result_json)
  
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return {"Message" :"An ERROR occurred in ETL GET_All Method",
                "Status"  : 500}


@app.route('/Etl_EstGetByID/<int:etl_estimator_ID>', methods=['GET'])
@background
def etl_Get_ByID_Estimator(etl_estimator_ID):
    try:
        app.logger.info('ETL GET by ID Process Starting')
        con = DataBase.getConnection()
        cur = con.cursor()
        if 'Admin' not in role:
            cur.execute("SELECT * FROM etl_estimator WHERE created_by = %s", [created_by])
            cur.fetchall()
            if cur.rowcount==0:
                  app.logger.info('Record Not Found for the Specific ETL ID')
                  #return jsonify("Record not found"), 404
                  return jsonify([])
        rows = cur.execute("""SELECT JSON_OBJECT(
                                'categoryId',e.category_id,
                                'categoryName',c.category_name,
                                'etlEstimatorId', e.etl_estimator_ID,
                                'projectName', e.projectName,
                                'estimatorName', e.estimatorName,
                                'etlName', e.etlName,
                                'totalEffortsInPersonHours', e.totalEfforts_inPersonHours,
                                'retestingEfforts', e.retestingEfforts,
                                'totalEffortsInPersonDays', e.totalEfforts_inPersonDays,
                                'createdDate',e.created_date,
                                'updatedDate',e.updated_date,
                                'isActive',e.is_active,
                                'etlTaskGroups', 
                                (SELECT JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'etlTaskGroupId', tg.etl_taskGroup_id, 
                                        'taskGroupId', tg.taskgroup_id,
                                        'taskGroupName', tg1.taskgroup_name,
                                        'etlEstimatorId',tg.etl_estimator_ID,
                                        'createdDate',tg.created_date,
                                        'updatedDate',tg.updated_date,
                                        'isActive',tg.is_active,
                                        'etlTaskLists', 
                                        (SELECT JSON_ARRAYAGG(
                                            JSON_OBJECT(
                                                'etlTaskListId', t.etl_tasklist_id,
                                                'taskListId',t.tasklist_id, 
                                                'etlTaskGroupId',t.etl_taskGroup_id,
                                                'taskName', t2.task_name, 
                                                'simple', t.simple, 
                                                'medium', t.medium, 
                                                'complex', t.complex,
                                                'simpleWf', t.simpleWF, 
                                                'mediumWf', t.mediumWF, 
                                                'complexWf', t.complexWF,
                                                'effortDays', t.effort_days, 
                                                'effortHours', t.effort_hours, 
                                                'createdDate',t.created_date,
                                                'updatedDate',t.updated_date,
                                                'isActive',t.is_active
                                            )
                                        ) FROM etl_tasklist t inner join tbltasklist t2 on t2.tasklist_id = t.tasklist_id WHERE t.etl_taskGroup_id = tg.etl_taskGroup_id and t.is_active=1 and t2.is_active=1)
                                    )
                                  ) FROM etl_taskgroup tg inner join tbltaskgroup tg1 on tg1.taskgroup_id = tg.taskgroup_id WHERE tg.etl_estimator_ID = e.etl_estimator_ID and tg.is_active=1 and tg1.is_active= 1)
                              ) FROM etl_estimator e  inner join category c on c.category_id = e.category_id WHERE e.etl_estimator_ID = %s and e.is_active=1 and c.is_active=1 AND CASE WHEN %s = '' THEN 1=1 ELSE e.created_by = %s END """, (etl_estimator_ID,created_by,created_by,))
        rows = cur.fetchall()
        if len(rows) == 0:
            app.logger.info('Record Not Found for the Specific ETL Id')
            return jsonify("Enter a valid ETL Estimator ID")
        con.close()
        result_json_str = rows[0][0]
        result_json = json.loads(result_json_str)
        app.logger.info('Getting ID request received Successfully')
        return jsonify(f"Showing ETL ESTIMATOR ID : {etl_estimator_ID}", result_json)
  
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return {"Message" :"An ERROR occurred in ETL GET_BY_ID Method",
                "Status"  : 500}

@app.route('/Etl_GetFilterValues/<int:category_id>', methods=['GET'])
def etl_getFilterValues(category_id):
    try:
        app.logger.info('Etl_GetFilterValues Process Starting')
        con = DataBase.getConnection()
        cur = con.cursor()
        cur.execute(""" SELECT JSON_ARRAYAGG(
                            JSON_OBJECT(
                                'categoryID', c.category_id,
                                'categoryName', c.category_name,
                                'estimator', (
                                    SELECT JSON_ARRAYAGG(
                                        JSON_OBJECT(
                                            'estimatorID', e.etl_estimator_id,
                                            'estimatorName', e.estimatorName,
                                            'taskgroup', (
                                                SELECT JSON_ARRAYAGG(
                                                    JSON_OBJECT(
                                                        'taskgroupID', tg.etl_taskgroup_id,
                                                        'taskgroupName', t.taskgroup_name
                                                    )
                                                )
                                                FROM etl_taskgroup tg
                                                INNER JOIN tbltaskgroup t ON tg.taskgroup_id = t.taskgroup_id
                                                WHERE tg.etl_estimator_id = e.etl_estimator_id
                                                AND t.is_active = 1
                                                AND tg.is_active = 1
                                            )
                                        )
                                    )
                                    FROM etl_estimator e
                                    WHERE c.category_id = e.category_id
                                    AND e.is_active = 1
                                )
                            )
                        ) AS json_data
                        FROM category c
                        WHERE c.category_id = %s
                        and c.is_active = 1""",(category_id,))
       
        rows = cur.fetchall()
        if len(rows) == 0:
            app.logger.info('Record Not Found for this Specific ETL category_id')
            return jsonify("please enter a valid category_id")
        con.close()
        result_json_str = rows[0][0]
        result_json = json.loads(result_json_str)
        app.logger.info('Etl_GetFilterValues request received Successfully')
        return jsonify(f"Showing category_id : {category_id}", result_json)
  

    except Exception as e:
        app.logger.error(f'Error: {str(e)}')
        return {"Message" :"An Error Occured in Getting Etl_GetFilterValues",
                "Status"  : 500}

@app.route('/Get_Etl_Wf_Values/<int:category_id>', methods=['GET'])
def get_Etl_Wf_Values(category_id):
  try:
    app.logger.info('Get_Etl_Wf_Values Process Starting')
    con = DataBase.getConnection()
    cur = con.cursor()
    cur.execute("""SELECT JSON_ARRAYAGG(
                            JSON_OBJECT('categoryID',C.category_id,
                                        'categoryName',C.category_name,
                                        'Workfactor',(
                                        SELECT JSON_ARRAYAGG(
												JSON_OBJECT('workfactor_id',twf.workfactor_id,
															'simpleWF',twf.simple_WF,
															'mediumWF',twf.medium_WF,
															'complexWF',twf.complex_WF
                                        )
                                    )FROM tblworkfactor AS twf Where twf.category_id = C.category_id
                                    )
							)
                            )as jsondata FROM category C where C.category_id = %s""",(category_id,))
    rows = cur.fetchall()
    if len(rows) == 0:
        app.logger.info('Record Not Found for this Specific QA category_id')
        return jsonify("please enter a valid category_id")
    con.close()
    result_json_str = rows[0][0]
    result_json = json.loads(result_json_str)
    app.logger.info('Get_Etl_Wf_Values request received Successfully')
    return jsonify(f"Showing category_id : {category_id}", result_json)

  except Exception as e:
    app.logger.error('An error occurred: %s', str(e))
    return {"Message" :"An Error Occured in Getting Get_Etl_Wf_Values",
            "Status"  : 500}

@app.route('/Etl_Estimator_Updt_Delete', methods=['PUT'])
@background
def etl_updateInsert_Estimator():
    try:
        app.logger.info('ETL_Updt_Delete Method Starting')
        request1 = request.get_json()
        for lst in request1:
            etlEstimatorId = lst.get("etlEstimatorId")
            categoryId=lst["categoryId"]
            projectName = lst["projectName"]
            estimatorName = lst["estimatorName"]
            etlName = lst["etlName"]
            totalEffortsInPersonHours = lst["totalEffortsInPersonHours"]
            retestingEfforts = lst["retestingEfforts"]
            totalEffortsInPersonDays = lst["totalEffortsInPersonDays"]
            updatedDate = datetime.now()
            etlTaskGroups = lst["etlTaskGroups"]
            isActive=lst["isActive"]
            app.logger.info('Data update request received on ETL Table')
            con = DataBase.getConnection()
            cur = con.cursor()        
            if etlEstimatorId is not None and etlEstimatorId != "":
                sql = """UPDATE etl_estimator SET projectName=%s, estimatorName=%s, etlName=%s, 
                         totalEfforts_inPersonHours=%s, retestingEfforts=%s, totalEfforts_inPersonDays=%s,
                         updated_date=%s,is_active=%s WHERE  etl_estimator_ID=%s """
                cur.execute(sql, (projectName, estimatorName, etlName, totalEffortsInPersonHours,
                                  retestingEfforts, totalEffortsInPersonDays, updatedDate,isActive, etlEstimatorId))
                app.logger.info("ETL estimator_ID Data Updated Successfully")
            else:
                sql = """INSERT INTO etl_estimator(category_id,projectName, estimatorName, etlName, 
                         totalEfforts_inPersonHours, retestingEfforts, totalEfforts_inPersonDays,is_active,created_by)
                         VALUES (%s, %s, %s, %s, %s, %s,%s,%s,%s)"""
                cur.execute(sql, (categoryId,projectName, estimatorName, etlName, totalEffortsInPersonHours,
                                  retestingEfforts, totalEffortsInPersonDays,isActive,created_by))
                etlEstimatorId = cur.lastrowid
                app.logger.info('ETL Estimator Data Newly Inserted Successfully By PUT Method')
                
            for lst in etlTaskGroups:
                etlTaskGroupId = lst.get("etlTaskGroupId")
                if etlTaskGroupId is not None and etlTaskGroupId != "":
                    cur.execute('UPDATE etl_taskgroup SET taskgroup_id=%s,updated_date=%s,is_active=%s WHERE etl_taskGroup_id=%s',
                                (lst['taskGroupId'],updatedDate,lst['isActive'], etlTaskGroupId))
                    app.logger.info("ETL_Taskgroup Data Updated Successfully")
                    
                else:
                    cur.execute('INSERT INTO etl_taskgroup (is_active,taskgroup_id, etl_estimator_ID) VALUES (%s, %s, %s)',
                                (lst['isActive'],lst["taskGroupId"], etlEstimatorId))
                    etlTaskGroupId = cur.lastrowid
                    app.logger.info('ETL TaskGroup Data Newly Inserted Successfully By PUT Method')
                    
                for tsklist in lst["etlTaskLists"]:
                    etlTaskListId = tsklist.get("etlTaskListId")
                    if etlTaskListId is not None and etlTaskListId != "":
                        updatedEffortDays = tsklist['simple']*tsklist['simpleWf'] + tsklist['medium']*tsklist['mediumWf'] + tsklist['complex']*tsklist['complexWf']
                        updatedEffortHours = updatedEffortDays*8
                        cur.execute('UPDATE etl_tasklist SET tasklist_id=%s, simple=%s, medium=%s, complex=%s, simpleWF=%s, mediumWF=%s, complexWF=%s, effort_days=%s, effort_hours=%s,updated_date=%s,is_active=%s WHERE etl_tasklist_id=%s',
                                    (tsklist['taskListId'], tsklist['simple'], tsklist['medium'], tsklist['complex'], tsklist['simpleWf'], tsklist['mediumWf'], tsklist['complexWf'], updatedEffortDays, updatedEffortHours, updatedDate,tsklist['isActive'], etlTaskListId))
                        app.logger.info("ETL_tasklist_id Data Updated Successfully")
                    
                    else:
                        effort_result_days = tsklist['simple']*tsklist['simpleWf'] + tsklist['medium']*tsklist['mediumWf'] + tsklist['complex']*tsklist['complexWf']
                        effort_result_hrs = effort_result_days*8
                        cur.execute('INSERT INTO etl_tasklist(tasklist_id,simple, medium, complex, simpleWF, mediumWF, complexWF, effort_days, effort_hours, is_active,etl_taskGroup_id) '
                                    'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                    (tsklist['taskListId'], tsklist['simple'], tsklist['medium'], tsklist['complex'], tsklist['simpleWf'], tsklist['mediumWf'], tsklist['complexWf'],
                                      effort_result_days, effort_result_hrs,tsklist['isActive'], etlTaskGroupId))
                        app.logger.info('etl_tasklist Data Newly Inserted Successfully By PUT Method')
            con.commit()
            con.close()
            app.logger.info('ETL UPDATE AND INSERT Process Successfully Executed')
            return {"Message" :"Etl Data Inserted Or Updated Successfully",
                    "Status"  : 200}
        
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return {"Message" :"An ERROR occurred in  Etl Estimator_Updt_Delete Method",
                "Status"  : 500}
    
@app.route('/Etl_download_excel_api/<int:category_id>/<estimator_ids>',methods=['GET'])
def etl_downloadExcelApi(category_id, estimator_ids):
    try:
        app.logger.info("Starting Excel function For ETL Table")
        query = etl_generateQuery(category_id, estimator_ids)
        file_path = etl_writeExcelFile(query)
        app.logger.info("Excel File Returning process successfully executed")
        filename = 'ETLdata.xlsx'
        response = make_response(send_file(file_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))
        response.headers['Content-Disposition'] = f"attachment; filename={filename}"
        return response

    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return {"Message" :"An ERROR occurred in downloadExcel ETL function",
                "Status"  : 500}

def etl_generateQuery(category_id, estimator_ids):
    query = """
        SELECT  c.category_name,es.projectName,es.estimatorName,es.totalEfforts_inPersonHours,es.retestingEfforts,es.totalEfforts_inPersonDays,tg.taskgroup_name,
        tl2.task_name,tl.simple, tl.medium, tl.complex, tl.simpleWF, tl.mediumWF,tl.complexWF, tl.effort_days, tl.effort_hours
        FROM category c
        INNER JOIN tbltaskgroup tg ON c.category_id = tg.category_id
        INNER JOIN etl_estimator es ON es.category_id = c.category_id
        INNER JOIN etl_taskgroup tg1 ON tg1.etl_estimator_ID = es.etl_estimator_ID
        AND tg1.taskgroup_id = tg.taskgroup_id
        INNER JOIN etl_tasklist tl ON tl.etl_taskGroup_id = tg1.etl_taskGroup_id
        INNER JOIN tbltasklist tl2 ON tl2.tasklist_id=tl.tasklist_id
        WHERE c.category_id = {}
        AND c.is_active = 1
        AND tg.is_active = 1
        AND tg1.is_active = 1
        AND tl.is_active = 1
        AND tl2.is_active = 1
        AND es.is_active = 1
        AND es.etl_estimator_ID IN ({})
    """.format(category_id,estimator_ids)
    return query

def etl_writeExcelFile(query):
    try:
        app.logger.info("Writing data to Excel file for ETL")
        con = DataBase.getConnection()
        cur = con.cursor()

        # Create a temporary file path
        temp_dir = os.path.join(app.instance_path, 'temp')
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = os.path.join(temp_dir, 'ETLdata.xlsx')
        workbook = xlsxwriter.Workbook(temp_file)
        worksheet = workbook.add_worksheet()
        header_format = workbook.add_format(
            {'bold': True,
             'bg_color': '#F0FFFF',
             'border': 2
            })
        header_format2 = workbook.add_format({'bold': True, 'bg_color': '#EE4B2B','font_color': '#ffffff','border': 2})
        border_format = workbook.add_format({'border': 2,"align": "Left"})
        merge_format = workbook.add_format(
                                      {
                                          "bold": 1,
                                          "border": 1,
                                          "align": "Left",
                                          "valign": "vcenter",
                                      }
                                  )
        # Merge cells for the image
        worksheet.merge_range("A1:J3", "", merge_format)
        worksheet.insert_image("A1",r"Image/emergere-logo.png",{"x_scale": 0.2, "y_scale": 0.2, "x_offset": 320, "y_offset": 10})
        worksheet.set_column(0,1,30)
        cur.execute(query)
        rows = cur.fetchall()
        if not rows:
            app.logger.warning("No rows returned from the query")
            workbook.close()
            return jsonify(message="No data available for the given parameters")
        # Write main headers
        worksheet.write(3, 0, 'Category Name', header_format)
        worksheet.write(4, 0, 'Project Name', header_format)
        worksheet.write(5, 0, 'Estimator Name', header_format)
        worksheet.write(6, 0,  'Total Efforts In Person Hours', header_format)
        worksheet.write(7, 0,  'Retesting Efforts', header_format)
        worksheet.write(8, 0,  'Total Efforts In Person Days', header_format)
        # Write header values
        category_name = rows[0][0]
        project_name = rows[0][1]
        estimator_name = rows[0][2]
        TotalEffortsInPersonHours=rows[0][3]
        RetestingEfforts=rows[0][4]
        TotalEffortsInPersonDays=rows[0][5]
        worksheet.merge_range("B4:J4", category_name, merge_format,)
        worksheet.merge_range("B5:J5", project_name, merge_format)
        worksheet.merge_range("B6:J6", estimator_name, merge_format)
        worksheet.merge_range("B7:J7", TotalEffortsInPersonHours, merge_format)
        worksheet.merge_range("B8:J8", RetestingEfforts, merge_format)
        worksheet.merge_range("B9:J9", TotalEffortsInPersonDays, merge_format)
        #worksheet.merge_range(0,7, c)
        headers=['Task Group Name','Task Name', 'Simple', 'Medium', 'Complex', 'Simple WF', 'Medium WF', 'Complex WF', 'Effort Days', 'Effort Hours',]
        for col, header_text in enumerate(headers):
           worksheet.write(9, col, header_text, header_format2)
        row_num = 10  # Starting row number for data

        for row_data in rows:
            worksheet.write_row(row_num, 0, row_data[6:], border_format)  # Write remaining data rows
            row_num += 1

        workbook.close()
        app.logger.info("Returning the Excel data")
        return temp_file

    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return {"Message" :"An ERROR occurred in etl_WriteExcelFile method",
                "Status"  : 500}

#---------------------------------------------------QA Estimator---------------------------------------------------
@app.route('/Qa_Est_Getall',methods=['GET'])
@background
def qa_Get_allEst_tables():
    try:
        app.logger.info('QA_Get All Process Starting')
        con = DataBase.getConnection()
        cur = con.cursor()
        if 'Admin' not in role:
            cur.execute("SELECT * FROM qa_estimator WHERE created_by = %s", [created_by])
            cur.fetchall()
            if cur.rowcount==0:
                  app.logger.info('Record Not Found for the New user')
                  #return jsonify("Record not found"), 404                  
                  return jsonify([])
        cur.execute  (""" SELECT JSON_ARRAYAGG(  
                              JSON_OBJECT(
                              'categoryId',e.category_id,
                              'categoryName',c.category_name,
                              'qaEstimatorId', e.qa_estimator_ID,
                              'projectName', e.projectName,
                              'estimatorName', e.estimatorName,
                              'qaName', e.qaName,
                              'totalEffortsInPersonHours', e.totalEfforts_inPersonHours,
                              'retestingEfforts', e.retestingEfforts,
                              'totalEffortsInPersonDays', e.totalEfforts_inPersonDays,
                              'createdDate',e.created_date,
                              'updatedDate',e.updated_date,
                              'isActive',e.is_active,
                              'qaTaskGroups', 
                          (SELECT JSON_ARRAYAGG(
                               JSON_OBJECT(
                                        'qaTaskGroupId', tg.qa_taskGroup_id,
                                        'taskGroupId',tg.taskgroup_id,
                                        'taskGroupName', tg1.taskgroup_name,
                                        'qaEstimatorId',tg.qa_estimator_ID,
                                        'createdDate',tg.created_date,
                                        'updatedDate',tg.updated_date,
                                        'isActive',tg.is_active,
                                        'qaTasksLists', 
                                      (SELECT JSON_ARRAYAGG(
                                           JSON_OBJECT(
                                                'qaTaskListId', t.qa_tasklist_id, 
                                                'taskListId',t.tasklist_id,
                                                'qaTaskGroupId',t.qa_taskGroup_id,
                                                'taskName', t2.task_name, 
                                                'simple', t.simple, 
                                                'medium', t.medium, 
                                                'complex', t.complex,
                                                'simpleWf', t.simpleWF, 
                                                'mediumWf', t.mediumWF, 
                                                'complexWf', t.complexWF,
                                                'effortDays', t.effort_days, 
                                                'effortHours', t.effort_hours, 
                                                'createdDate',t.created_date,
                                                'updatedDate',t.updated_date,
                                                'isActive',t.is_active
                        )
                   ) FROM qa_tasklist t inner join tbltasklist t2 on t2.tasklist_id = t.tasklist_id  where t.qa_taskGroup_id =tg.qa_taskGroup_id and t.is_active=1 and t2.is_active=1)
              )
         ) FROM qa_taskgroup tg inner join tbltaskgroup tg1 on tg1.taskgroup_id = tg.taskgroup_id WHERE tg.qa_estimator_ID = e.qa_estimator_ID and tg.is_active=1 and tg1.is_active= 1)
        )
        )FROM qa_estimator e  inner join category c on c.category_id = e.category_id WHERE e.is_active=1 and c.is_active=1 AND CASE WHEN %s = '' THEN 1=1 ELSE e.created_by = %s END """,(created_by,created_by,))
        rows = cur.fetchall()
        result_json_str=rows[0][0]
        result_json = json.loads(result_json_str)
        app.logger.info('QA_Get All request received Successfully')
        return jsonify(result_json)
  
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return { "Message" :"An ERROR occurred in QA GET_All Method",
                 "Status"  : 500}

@app.route('/Qa_EstGetByID/<int:qa_estimator_ID>', methods=['GET'])
@background
def qa_Get_ByID_Estimator(qa_estimator_ID):
    try:
        app.logger.info('QA_Get_By_ID Process Starting')
        con = DataBase.getConnection()
        cur = con.cursor()
        if 'Admin' not in role:
            cur.execute("SELECT * FROM qa_estimator WHERE created_by = %s", [created_by])
            cur.fetchall()
            if cur.rowcount==0:
                  app.logger.info('Record Not Found for the New user')
                  #return jsonify("Record not found"), 404
                  return jsonify([])
        rows = cur.execute("""SELECT JSON_OBJECT(
                                'categoryId',e.category_id,
                                'categoryName',c.category_name,
                                'qaEstimatorId', e.qa_estimator_ID,
                                'projectName', e.projectName,
                                'estimatorName', e.estimatorName,
                                'qaName', e.qaName,
                                'totalEffortsInPersonHours', e.totalEfforts_inPersonHours,
                                'retestingEfforts', e.retestingEfforts,
                                'totalEffortsInPersonDays', e.totalEfforts_inPersonDays,
                                'createdDate',e.created_date,
                                'updatedDate',e.updated_date,
                                'isActive',e.is_active,
                                'qaTaskGroups', 
                                (SELECT JSON_ARRAYAGG(
                                    JSON_OBJECT(
                                        'qaTaskGroupId', tg.qa_taskGroup_id, 
                                        'taskGroupId', tg.taskgroup_id,
                                        'taskGroupName', tg1.taskgroup_name,
                                        'qaEstimatorId',tg.qa_estimator_ID,
                                        'createdDate',tg.created_date,
                                        'updatedDate',tg.updated_date,
                                        'isActive',tg.is_active,
                                        'qaTasksLists', 
                                        (SELECT JSON_ARRAYAGG(
                                            JSON_OBJECT(
                                                'qaTaskListId', t.qa_tasklist_id,
                                                'taskListId',t.tasklist_id, 
                                                'qaTaskGroupId',t.qa_taskGroup_id,
                                                'taskName', t2.task_name, 
                                                'simple', t.simple, 
                                                'medium', t.medium, 
                                                'complex', t.complex,
                                                'simpleWf', t.simpleWF, 
                                                'mediumWf', t.mediumWF, 
                                                'complexWf', t.complexWF,
                                                'effortDays', t.effort_days, 
                                                'effortHours', t.effort_hours, 
                                                'createdDate',t.created_date,
                                                'updatedDate',t.updated_date,
                                                'isActive',t.is_active
                                            )
                                        ) FROM qa_tasklist t inner join tbltasklist t2 on t2.tasklist_id = t.tasklist_id WHERE t.qa_taskGroup_id = tg.qa_taskGroup_id and t.is_active=1 and t2.is_active=1)
                                    )
                                  ) FROM qa_taskgroup tg inner join tbltaskgroup tg1 on tg1.taskgroup_id = tg.taskgroup_id WHERE tg.qa_estimator_ID = e.qa_estimator_ID and tg.is_active=1 and tg1.is_active= 1)
                                ) FROM qa_estimator e  inner join category c on c.category_id = e.category_id WHERE e.qa_estimator_ID = %s and e.is_active=1 and c.is_active=1 AND CASE WHEN %s = '' THEN 1=1 ELSE e.created_by = %s END """, (qa_estimator_ID,created_by,created_by,))           
        rows = cur.fetchall()
        if len(rows) == 0:
            app.logger.info('Record Not Found for this Specific QA_Id')
            return jsonify("please enter a valid qa_estimator_ID")
        con.close()
        result_json_str = rows[0][0]
        result_json = json.loads(result_json_str)
        app.logger.info('QA Get_By_ID request received Successfully')
        return jsonify(f"Showing QA_estimator_ID : {qa_estimator_ID}", result_json)
  
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return { "Message" :"An ERROR occurred in QA GET_BY_ID Method",
                 "Status"  : 500}
  
@app.route('/Qa_GetFilterValues/<int:category_id>', methods=['GET'])
def qa_getFilterValues(category_id):
    try:
        app.logger.info('Qa_GetFilterValues Process Starting')
        con = DataBase.getConnection()
        cur = con.cursor()
        cur.execute(""" SELECT JSON_ARRAYAGG(
                           JSON_OBJECT(
                               'categoryID', c.category_id,
                               'categoryName', c.category_name,
                               'estimator', (
                                   SELECT JSON_ARRAYAGG(
                                       JSON_OBJECT(
                                           'estimatorID', e.qa_estimator_ID,
                                            'estimatorName', e.estimatorName,
                                           'taskgroup', (
                                               SELECT JSON_ARRAYAGG(
                                                   JSON_OBJECT(
                                                       'taskgroupID', tg.qa_taskGroup_id,
                                                       'taskgroupName', t.taskgroup_name
                                                   )
                                               )
                                               FROM qa_taskgroup tg
                                               inner join tbltaskgroup t
                                               on tg.taskgroup_id = t.taskgroup_id
                                               WHERE tg.qa_estimator_ID = e.qa_estimator_ID
                                               and t.is_active = 1
                                               and tg.is_active = 1
                                           )
                                       )
                                   )
                                FROM qa_estimator e
                                WHERE c.category_id = e.category_id
                                and e.is_active = 1
                               )
                           )
                       ) AS json_data
                FROM category c
                where c.category_id = %s 
                and c.is_active = 1""",(category_id,))
       
        rows = cur.fetchall()
        if len(rows) == 0:
            app.logger.info('Record Not Found for this Specific category_id')
            return jsonify("please enter a valid category_id")
        con.close()
        result_json_str = rows[0][0]
        result_json = json.loads(result_json_str)
        app.logger.info('Qa_GetFilterValues request received Successfully')
        return jsonify(f"Showing category_id : {category_id}", result_json)

    except Exception as e:
        app.logger.error(f'Error: {str(e)}')
        return {"Message" :"An Error Occured in Getting Qa_GetFilterValues",
                "Status"  : 500}

@app.route('/Get_Qa_Wf_Values/<int:category_id>', methods=['GET'])
def get_Qa_Wf_Values(category_id):
    try:
      app.logger.info('Get_Qa_Wf_Values Process Starting')
      con = DataBase.getConnection()
      cur = con.cursor()
      cur.execute("""SELECT JSON_ARRAYAGG(
                              JSON_OBJECT('categoryID',C.category_id,
                                          'categoryName',C.category_name,
                                          'Workfactor',(
                                          SELECT JSON_ARRAYAGG(
    										JSON_OBJECT('workfactor_id',twf.workfactor_id,
    													'simpleWF',twf.simple_WF,
    													'mediumWF',twf.medium_WF,
    													'complexWF',twf.complex_WF
                                          )
                                      )FROM tblworkfactor AS twf Where twf.category_id = C.category_id
                                      )
    					)
                              )as jsondata FROM category C where C.category_id = %s""",(category_id,))
      rows = cur.fetchall()
      if len(rows) == 0:
          app.logger.info('Record Not Found for this Specific Qa_Id')
          return jsonify("please enter a valid category_id")
      con.close()
      result_json_str = rows[0][0]
      result_json = json.loads(result_json_str)
      app.logger.info('Get_Qa_Wf_Values request received Successfully')
      return jsonify(f"Showing category_id : {category_id}", result_json)
    
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return {"Message" :"An Error Occured in Getting Get_Qa_Wf_Values",
                "Status"  : 500}

@app.route('/Qa_Estimator_Updt_Delete', methods=['PUT'])
@background
def qa_updateInsert_Estimator():
    try:
        app.logger.info('Qa_Estimator_Updt_Delete Method Starting')
        request1 = request.get_json()
        for lst in request1:
            qaEstimatorId = lst.get("qaEstimatorId")
            categoryId=lst["categoryId"]
            projectName = lst["projectName"]
            estimatorName = lst["estimatorName"]
            qaName = lst["qaName"]
            totalEffortsInPersonHours = lst["totalEffortsInPersonHours"]
            retestingEfforts = lst["retestingEfforts"]
            totalEffortsInPersonDays = lst["totalEffortsInPersonDays"]
            updatedDate = datetime.now()
            qaTaskGroups = lst["qaTaskGroups"]
            isActive=lst["isActive"]
            app.logger.info('Data update request received')
            con = DataBase.getConnection()
            cur = con.cursor()            
            if qaEstimatorId is not None and qaEstimatorId != "":
                sql = """UPDATE qa_estimator SET projectName=%s, estimatorName=%s, qaName=%s, category_id=%s,
                         totalEfforts_inPersonHours=%s, retestingEfforts=%s, totalEfforts_inPersonDays=%s,
                         updated_date=%s,is_active=%s WHERE  qa_estimator_ID=%s """
                cur.execute(sql, (projectName, estimatorName, qaName,categoryId, totalEffortsInPersonHours,
                                  retestingEfforts, totalEffortsInPersonDays, updatedDate,isActive, qaEstimatorId))
                app.logger.info("QA_estimator Data Updated Successfully")

            else:
                sql = """INSERT INTO qa_estimator(category_id,projectName, estimatorName, qaName, 
                         totalEfforts_inPersonHours, retestingEfforts, totalEfforts_inPersonDays,is_active,created_by)
                         VALUES (%s, %s, %s, %s, %s, %s,%s,%s,%s)"""
                cur.execute(sql, (categoryId,projectName, estimatorName, qaName, totalEffortsInPersonHours,
                                  retestingEfforts, totalEffortsInPersonDays,isActive,created_by))
                qaEstimatorId = cur.lastrowid
                app.logger.info('QA_estimator Data Newly Inserted Successfully By PUT Method')
            for lst in qaTaskGroups:
                qaTaskGroupId = lst.get("qaTaskGroupId")
                if qaTaskGroupId is not None and qaTaskGroupId != "":
                    cur.execute('UPDATE qa_taskgroup SET taskgroup_id=%s,updated_date=%s,is_active=%s WHERE qa_taskGroup_id=%s',
                                (lst['taskGroupId'],updatedDate,lst['isActive'], qaTaskGroupId))
                    app.logger.info("QA_taskgroup  Data Updated Successfully")
                else:
                    cur.execute('INSERT INTO qa_taskgroup(is_active,taskgroup_id, qa_estimator_ID) VALUES (%s, %s,%s)',
                                (lst['isActive'],lst["taskGroupId"], qaEstimatorId))
                    qaTaskGroupId = cur.lastrowid
                    app.logger.info('QA_taskgroup Data Newly Inserted Successfully By PUT Method')
                for tsklist in lst["qaTasksLists"]:
                    qaTaskListId = tsklist.get("qaTaskListId")
                    if qaTaskListId is not None and qaTaskListId != "":
                        updatedEffortDays = tsklist['simple']*tsklist['simpleWf'] + tsklist['medium']*tsklist['mediumWf'] + tsklist['complex']*tsklist['complexWf']
                        updatedEffortHours = updatedEffortDays*8
                        cur.execute('UPDATE qa_tasklist SET tasklist_id=%s, simple=%s, medium=%s, complex=%s, simpleWF=%s, mediumWF=%s, complexWF=%s, effort_days=%s, effort_hours=%s,updated_date=%s,is_active=%s WHERE qa_tasklist_id=%s',
                                    (tsklist['taskListId'], tsklist['simple'], tsklist['medium'], tsklist['complex'], tsklist['simpleWf'], tsklist['mediumWf'], tsklist['complexWf'], updatedEffortDays, updatedEffortHours, updatedDate,tsklist['isActive'], qaTaskListId))
                        app.logger.info("QA_tasklist Data Updated Successfully")
                    else:
                        effort_result_days = tsklist['simple']*tsklist['simpleWf'] + tsklist['medium']*tsklist['mediumWf'] + tsklist['complex']*tsklist['complexWf']
                        effort_result_hrs = effort_result_days*8
                        cur.execute('INSERT INTO qa_tasklist(tasklist_id,simple, medium, complex, simpleWF, mediumWF, complexWF, effort_days, effort_hours, is_active,qa_taskGroup_id) '
                                    'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                    (tsklist['taskListId'], tsklist['simple'], tsklist['medium'], tsklist['complex'], tsklist['simpleWf'], tsklist['mediumWf'], tsklist['complexWf'],
                                      effort_result_days, effort_result_hrs,tsklist['isActive'], qaTaskGroupId))
                        app.logger.info('QA_tasklist Data Newly Inserted Successfully By PUT Method')
            con.commit()
            con.close()
            app.logger.info('Qa_Estimator_Updt_Delete Process Successfully Executed')
            return {"Message" :"Qa Data Inserted Or Updated Successfully",
                    "Status"  : 200}
            
    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return {"Message" :"An ERROR occurred in Qa Estimator_Updt_Delete Method",
                "Status"  : 500}

@app.route('/Qa_download_excel_api/<int:category_id>/<estimator_ids>')
def qa_downloadExcelApi(category_id, estimator_ids):
    try:
        app.logger.info("Starting Qa_download_excel_api API function")
        query = qa_generateQuery(category_id, estimator_ids)
        file_path = qa_writeExcelFile(query)
        app.logger.info("Excel File Returning process successfully executed")
        filename = 'Qa_data.xlsx'
        response = make_response(send_file(file_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))
        response.headers['Content-Disposition'] = f"attachment; filename={filename}"
        return response

    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return {"Message" :"An ERROR occurred in qa_downloadExcelApi function",
                "Status"  : 500}

def qa_generateQuery(category_id, estimator_ids):
    query = """
        SELECT  c.category_name,es.projectName,es.estimatorName,es.totalEfforts_inPersonHours,es.retestingEfforts,es.totalEfforts_inPersonDays,tg.taskgroup_name,
         tl2.task_name,tl.simple, tl.medium, tl.complex, tl.simpleWF, tl.mediumWF,tl.complexWF, tl.effort_days, tl.effort_hours
        FROM category c
        INNER JOIN tbltaskgroup tg ON c.category_id = tg.category_id
        INNER JOIN qa_estimator es ON es.category_id = c.category_id
        INNER JOIN qa_taskgroup tg1 ON tg1.qa_estimator_ID = es.qa_estimator_ID
        AND tg1.taskgroup_id = tg.taskgroup_id
        INNER JOIN qa_tasklist tl ON tl.qa_taskGroup_id = tg1.qa_taskGroup_id
        INNER JOIN tbltasklist tl2 ON tl2.tasklist_id=tl.tasklist_id
        WHERE c.category_id = {}
        AND c.is_active = 1
        AND tg.is_active = 1
        AND tg1.is_active = 1
        AND tl.is_active = 1
        AND tl2.is_active = 1
        AND es.is_active = 1
        AND es.qa_estimator_ID IN ({})
    """.format(category_id,estimator_ids)
    return query

def qa_writeExcelFile(query):
    try:
        app.logger.info("Updating data to Excel file")
        con = DataBase.getConnection()
        cur = con.cursor()

        # Create a temporary file path
        temp_dir = os.path.join(app.instance_path, 'temp')
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = os.path.join(temp_dir, 'Qa_data.xlsx')
        workbook = xlsxwriter.Workbook(temp_file)
        worksheet = workbook.add_worksheet()
        header_format = workbook.add_format(
            {'bold': True,
             'bg_color': '#F0FFFF',
             'border': 2
            })
        header_format2 = workbook.add_format({'bold': True, 'bg_color': '#EE4B2B','font_color': '#ffffff','border': 2})
        border_format = workbook.add_format({'border': 2,"align": "Left"})
        merge_format = workbook.add_format(
                                      {
                                          "bold": 1,
                                          "border": 1,
                                          "align": "Left",
                                          "valign": "vcenter",
                                      }
                                  )
        # Merge cells for the image
        worksheet.merge_range("A1:J3", "", merge_format)
        worksheet.insert_image("A1",r"Image/emergere-logo.png",{"x_scale": 0.2, "y_scale": 0.2, "x_offset": 320, "y_offset": 10})
        worksheet.set_column(0,1,30)
        cur.execute(query)
        rows = cur.fetchall()
        if not rows:
            app.logger.warning("No rows returned from the query")
            workbook.close()
            return jsonify(message="No data available for the given parameters")
        # Write main headers
        worksheet.write(3, 0, 'Category Name', header_format)
        worksheet.write(4, 0, 'Project Name', header_format)
        worksheet.write(5, 0, 'Estimator Name', header_format)
        worksheet.write(6, 0,  'Total Efforts In Person Hours', header_format)
        worksheet.write(7, 0,  'Retesting Efforts', header_format)
        worksheet.write(8, 0,  'Total Efforts In Person Days', header_format)
        # Write header values
        category_name = rows[0][0]
        project_name = rows[0][1]
        estimator_name = rows[0][2]
        TotalEffortsInPersonHours=rows[0][3]
        RetestingEfforts=rows[0][4]
        TotalEffortsInPersonDays=rows[0][5]
        worksheet.merge_range("B4:J4", category_name, merge_format,)
        worksheet.merge_range("B5:J5", project_name, merge_format)
        worksheet.merge_range("B6:J6", estimator_name, merge_format)
        worksheet.merge_range("B7:J7", TotalEffortsInPersonHours, merge_format)
        worksheet.merge_range("B8:J8", RetestingEfforts, merge_format)
        worksheet.merge_range("B9:J9", TotalEffortsInPersonDays, merge_format)
        #worksheet.merge_range(0,7, c)
        headers=['Task Group Name','Task Name', 'Simple', 'Medium', 'Complex', 'Simple WF', 'Medium WF', 'Complex WF', 'Effort Days', 'Effort Hours',]
        for col, header_text in enumerate(headers):
           worksheet.write(9, col, header_text, header_format2)
        row_num = 10  # Starting row number for data

        for row_data in rows:
            worksheet.write_row(row_num, 0, row_data[6:], border_format)  # Write remaining data rows
            row_num += 1

        workbook.close()
        app.logger.info("Returning the Excel data")
        return temp_file

    except Exception as e:
        app.logger.error('An error occurred: %s', str(e))
        return {"Message" :"An ERROR occurred in the qa_writeExcelFile method",
                "Status"  : 500}

if __name__ == '__main__':
    app.run(debug=True)