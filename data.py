import mysql.connector

class DataBase:
    def getConnection():
        con = mysql.connector.connect(host='emergerejobcareer-mysql.mysql.database.azure.com',
                                      database='estimator',
                                      user='emergere',
                                      password='password-1')   

        return con
    getConnection()