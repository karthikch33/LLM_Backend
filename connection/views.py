from django.shortcuts import HttpResponse
from rest_framework.decorators import api_view
from .utils import sapnwrfc
from ctypes import *
from rest_framework.response import Response
from rest_framework import status
from hdbcli import dbapi
import sqlite3
from django.db import connections, transaction
from .serlializers import *
from .models import Project,Connection
import json
from django.core.serializers import serialize
import pandas as pd
import re,string
from django.db import connection
from rest_framework.views import APIView
from .serlializers import FileSerializer
from .models import FileConnection
from django.utils import timezone
from django.db.models import Q, Case, When, Value, IntegerField
import LLM_migration
from django.forms.models import model_to_dict

def home(request):
    return HttpResponse("home Page")


@api_view(['POST'])
def SAPconn(request):
    class RFC_ERROR_INFO(Structure):
        _fields_ = [("code", c_long),
                    ("group", c_long),
                    ("key", c_wchar * 128),
                    ("message", c_wchar * 512),
                    ("abapMsgClass", c_wchar * 21),
                    ("abapMsgType", c_wchar * 2),
                    ("abapMsgNumber", c_wchar * 4),
                    ("abapMsgV1", c_wchar * 51),
                    ("abapMsgV2", c_wchar * 51),
                    ("abapMsgV3", c_wchar * 51),
                    ("abapMsgV4", c_wchar * 51)]
    class RFC_CONNECTION_PARAMETER(Structure):
        _fields_ = [("name", c_wchar_p),
                    ("value", c_wchar_p)]
    RFC_OK = 0
    RFC_COMMUNICATION_FAILURE = 1
    RFC_LOGON_FAILURE = 2
    RFC_ABAP_RUNTIME_FAILURE = 3
    RFC_ABAP_MESSAGE = 4
    RFC_ABAP_EXCEPTION = 5
    RFC_CLOSED = 6
    RFC_CANCELED = 7
    RFC_TIMEOUT = 8
    RFC_MEMORY_INSUFFICIENT = 9
    RFC_VERSION_MISMATCH = 10
    RFC_INVALID_PROTOCOL = 11
    RFC_SERIALIZATION_FAILURE = 12
    RFC_INVALID_HANDLE = 13
    RFC_RETRY = 14
    RFC_EXTERNAL_FAILURE = 15
    RFC_EXECUTED = 16
    RFC_NOT_FOUND = 17
    RFC_NOT_SUPPORTED = 18
    RFC_ILLEGAL_STATE = 19
    RFC_INVALID_PARAMETER = 20
    RFC_CODEPAGE_CONVERSION_FAILURE = 21
    RFC_CONVERSION_FAILURE = 22
    RFC_BUFFER_TOO_SMALL = 23
    RFC_TABLE_MOVE_BOF = 24
    RFC_TABLE_MOVE_EOF = 25
    RFC_START_SAPGUI_FAILURE = 26
    RFC_ABAP_CLASS_EXCEPTION = 27
    RFC_UNKNOWN_ERROR = 28
    RFC_AUTHORIZATION_FAILURE = 29

    #-RFCTYPE - RFC data types----------------------------------------------
    RFCTYPE_CHAR = 0
    RFCTYPE_DATE = 1
    RFCTYPE_BCD = 2
    RFCTYPE_TIME = 3
    RFCTYPE_BYTE = 4
    RFCTYPE_TABLE = 5
    RFCTYPE_NUM = 6
    RFCTYPE_FLOAT = 7
    RFCTYPE_INT = 8
    RFCTYPE_INT2 = 9
    RFCTYPE_INT1 = 10
    RFCTYPE_NULL = 14
    RFCTYPE_ABAPOBJECT = 16
    RFCTYPE_STRUCTURE = 17
    RFCTYPE_DECF16 = 23
    RFCTYPE_DECF34 = 24
    RFCTYPE_XMLDATA = 28
    RFCTYPE_STRING = 29
    RFCTYPE_XSTRING = 30
    RFCTYPE_BOX = 31
    RFCTYPE_GENERIC_BOX = 32

    #-RFC_UNIT_STATE - Processing status of a background unit---------------
    RFC_UNIT_NOT_FOUND = 0 
    RFC_UNIT_IN_PROCESS = 1 
    RFC_UNIT_COMMITTED = 2 
    RFC_UNIT_ROLLED_BACK = 3 
    RFC_UNIT_CONFIRMED = 4 

    #-RFC_CALL_TYPE - Type of an incoming function call---------------------
    RFC_SYNCHRONOUS = 0 
    RFC_TRANSACTIONAL = 1 
    RFC_QUEUED = 2 
    RFC_BACKGROUND_UNIT = 3 

    #-RFC_DIRECTION - Direction of a function module parameter--------------
    RFC_IMPORT = 1 
    RFC_EXPORT = 2 
    RFC_CHANGING = RFC_IMPORT + RFC_EXPORT 
    RFC_TABLES = 4 + RFC_CHANGING 

    #-RFC_CLASS_ATTRIBUTE_TYPE - Type of an ABAP object attribute-----------
    RFC_CLASS_ATTRIBUTE_INSTANCE = 0 
    RFC_CLASS_ATTRIBUTE_CLASS = 1 
    RFC_CLASS_ATTRIBUTE_CONSTANT = 2 

    #-RFC_METADATA_OBJ_TYPE - Ingroup repository----------------------------
    RFC_METADATA_FUNCTION = 0 
    RFC_METADATA_TYPE = 1 
    RFC_METADATA_CLASS = 2 


    #-Variables-------------------------------------------------------------
    ErrInf = RFC_ERROR_INFO; RfcErrInf = ErrInf()
    ConnParams = RFC_CONNECTION_PARAMETER * 5; RfcConnParams = ConnParams()
    SConParams = RFC_CONNECTION_PARAMETER * 3; RfcSConParams = SConParams()


    SAPNWRFC = "sapnwrfc.dll"
    SAP = windll.LoadLibrary(SAPNWRFC)

    #-Prototypes------------------------------------------------------------
    SAP.RfcAppendNewRow.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcAppendNewRow.restype = c_void_p

    SAP.RfcCloseConnection.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcCloseConnection.restype = c_ulong

    SAP.RfcCreateFunction.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcCreateFunction.restype = c_void_p

    SAP.RfcCreateFunctionDesc.argtypes = [c_wchar_p, POINTER(ErrInf)]
    SAP.RfcCreateFunctionDesc.restype = c_void_p

    SAP.RfcDestroyFunction.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcDestroyFunction.restype = c_ulong

    SAP.RfcDestroyFunctionDesc.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcDestroyFunctionDesc.restype = c_ulong

    SAP.RfcGetChars.argtypes = [c_void_p, c_wchar_p, c_void_p, c_ulong, \
    POINTER(ErrInf)]
    SAP.RfcGetChars.restype = c_ulong

    SAP.RfcGetCurrentRow.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcGetCurrentRow.restype = c_void_p

    SAP.RfcGetFunctionDesc.argtypes = [c_void_p, c_wchar_p, POINTER(ErrInf)]
    SAP.RfcGetFunctionDesc.restype = c_void_p

    SAP.RfcGetRowCount.argtypes = [c_void_p, POINTER(c_ulong), \
    POINTER(ErrInf)]
    SAP.RfcGetRowCount.restype = c_ulong

    SAP.RfcGetStructure.argtypes = [c_void_p, c_wchar_p, \
    POINTER(c_void_p), POINTER(ErrInf)]
    SAP.RfcGetStructure.restype = c_ulong

    SAP.RfcGetTable.argtypes = [c_void_p, c_wchar_p, POINTER(c_void_p), \
    POINTER(ErrInf)]
    SAP.RfcGetTable.restype = c_ulong

    SAP.RfcGetVersion.argtypes = [POINTER(c_ulong), POINTER(c_ulong), \
    POINTER(c_ulong)]
    SAP.RfcGetVersion.restype = c_wchar_p

    SAP.RfcInstallServerFunction.argtypes = [c_wchar_p, c_void_p, \
    c_void_p, POINTER(ErrInf)]
    SAP.RfcInstallServerFunction.restype = c_ulong

    SAP.RfcInvoke.argtypes = [c_void_p, c_void_p, POINTER(ErrInf)]
    SAP.RfcInvoke.restype = c_ulong

    SAP.RfcListenAndDispatch.argtypes = [c_void_p, c_ulong, POINTER(ErrInf)]
    SAP.RfcListenAndDispatch.restype = c_ulong

    SAP.RfcMoveToFirstRow.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcMoveToFirstRow.restype = c_ulong

    SAP.RfcMoveToNextRow.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcMoveToNextRow.restype = c_ulong

    SAP.RfcOpenConnection.argtypes = [POINTER(ConnParams), c_ulong, \
    POINTER(ErrInf)]
    SAP.RfcOpenConnection.restype = c_void_p

    SAP.RfcPing.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcPing.restype = c_ulong

    SAP.RfcRegisterServer.argtypes = [POINTER(SConParams), c_ulong, \
    POINTER(ErrInf)]
    SAP.RfcRegisterServer.restype = c_void_p

    SAP.RfcSetChars.argtypes = [c_void_p, c_wchar_p, c_wchar_p, c_ulong, \
    POINTER(ErrInf)]
    SAP.RfcSetChars.restype = c_ulong

    RfcConnParams[0].name = "ASHOST"; RfcConnParams[0].value = request.data['host']
    RfcConnParams[1].name = "SYSNR" ; RfcConnParams[1].value = request.data['sysnr']            
    RfcConnParams[2].name = "CLIENT"; RfcConnParams[2].value = request.data['client']      
    RfcConnParams[3].name = "USER"  ; RfcConnParams[3].value = request.data['username']     
    RfcConnParams[4].name = "PASSWD"; RfcConnParams[4].value = request.data['password']  


    hRFC = SAP.RfcOpenConnection(RfcConnParams, 5, RfcErrInf)
    # hRFC = ""
    if hRFC != None:
        return Response(status=status.HTTP_200_OK)
    else:
        print(RfcErrInf.message)
    return Response(status=status.HTTP_404_NOT_FOUND)








@api_view(['GET'])
def saptables_to_sqlite(request):
   
 
   
    class RFC_ERROR_INFO(Structure):
        _fields_ = [("code", c_ulong),
                    ("group", c_ulong),
                    ("key", c_wchar * 128),
                    ("message", c_wchar * 512),
                    ("abapMsgClass", c_wchar * 21),
                    ("abapMsgType", c_wchar * 2),
                    ("abapMsgNumber", c_wchar * 4),
                    ("abapMsgV1", c_wchar * 51),
                    ("abapMsgV2", c_wchar * 51),
                    ("abapMsgV3", c_wchar * 51),
                    ("abapMsgV4", c_wchar * 51)]
 
    class RFC_CONNECTION_PARAMETER(Structure):
        _fields_ = [("name", c_wchar_p),
                    ("value", c_wchar_p)]
 
 
    #-Constants-------------------------------------------------------------
 
    #-RFC_RC - RFC return codes---------------------------------------------
    RFC_OK = 0
    RFC_COMMUNICATION_FAILURE = 1
    RFC_LOGON_FAILURE = 2
    RFC_ABAP_RUNTIME_FAILURE = 3
    RFC_ABAP_MESSAGE = 4
    RFC_ABAP_EXCEPTION = 5
    RFC_CLOSED = 6
    RFC_CANCELED = 7
    RFC_TIMEOUT = 8
    RFC_MEMORY_INSUFFICIENT = 9
    RFC_VERSION_MISMATCH = 10
    RFC_INVALID_PROTOCOL = 11
    RFC_SERIALIZATION_FAILURE = 12
    RFC_INVALID_HANDLE = 13
    RFC_RETRY = 14
    RFC_EXTERNAL_FAILURE = 15
    RFC_EXECUTED = 16
    RFC_NOT_FOUND = 17
    RFC_NOT_SUPPORTED = 18
    RFC_ILLEGAL_STATE = 19
    RFC_INVALID_PARAMETER = 20
    RFC_CODEPAGE_CONVERSION_FAILURE = 21
    RFC_CONVERSION_FAILURE = 22
    RFC_BUFFER_TOO_SMALL = 23
    RFC_TABLE_MOVE_BOF = 24
    RFC_TABLE_MOVE_EOF = 25
    RFC_START_SAPGUI_FAILURE = 26
    RFC_ABAP_CLASS_EXCEPTION = 27
    RFC_UNKNOWN_ERROR = 28
    RFC_AUTHORIZATION_FAILURE = 29
 
    #-RFCTYPE - RFC data types----------------------------------------------
    RFCTYPE_CHAR = 0
    RFCTYPE_DATE = 1
    RFCTYPE_BCD = 2
    RFCTYPE_TIME = 3
    RFCTYPE_BYTE = 4
    RFCTYPE_TABLE = 5
    RFCTYPE_NUM = 6
    RFCTYPE_FLOAT = 7
    RFCTYPE_INT = 8
    RFCTYPE_INT2 = 9
    RFCTYPE_INT1 = 10
    RFCTYPE_NULL = 14
    RFCTYPE_ABAPOBJECT = 16
    RFCTYPE_STRUCTURE = 17
    RFCTYPE_DECF16 = 23
    RFCTYPE_DECF34 = 24
    RFCTYPE_XMLDATA = 28
    RFCTYPE_STRING = 29
    RFCTYPE_XSTRING = 30
    RFCTYPE_BOX = 31
    RFCTYPE_GENERIC_BOX = 32
 
    #-RFC_UNIT_STATE - Processing status of a background unit---------------
    RFC_UNIT_NOT_FOUND = 0
    RFC_UNIT_IN_PROCESS = 1
    RFC_UNIT_COMMITTED = 2
    RFC_UNIT_ROLLED_BACK = 3
    RFC_UNIT_CONFIRMED = 4
 
    #-RFC_CALL_TYPE - Type of an incoming function call---------------------
    RFC_SYNCHRONOUS = 0
    RFC_TRANSACTIONAL = 1
    RFC_QUEUED = 2
    RFC_BACKGROUND_UNIT = 3
 
    #-RFC_DIRECTION - Direction of a function module parameter--------------
    RFC_IMPORT = 1
    RFC_EXPORT = 2
    RFC_CHANGING = RFC_IMPORT + RFC_EXPORT
    RFC_TABLES = 4 + RFC_CHANGING
 
    #-RFC_CLASS_ATTRIBUTE_TYPE - Type of an ABAP object attribute-----------
    RFC_CLASS_ATTRIBUTE_INSTANCE = 0
    RFC_CLASS_ATTRIBUTE_CLASS = 1
    RFC_CLASS_ATTRIBUTE_CONSTANT = 2
 
    #-RFC_METADATA_OBJ_TYPE - Ingroup repository----------------------------
    RFC_METADATA_FUNCTION = 0
    RFC_METADATA_TYPE = 1
    RFC_METADATA_CLASS = 2
 
 
    #-Variables-------------------------------------------------------------
    ErrInf = RFC_ERROR_INFO; RfcErrInf = ErrInf()
    ConnParams = RFC_CONNECTION_PARAMETER * 5; RfcConnParams = ConnParams()
    SConParams = RFC_CONNECTION_PARAMETER * 3; RfcSConParams = SConParams()
 
 
    #-Library---------------------------------------------------------------
    # if str(platform.architecture()[0]) == "32bit":
    #   os.environ['PATH'] += ";C:\\SAPRFCSDK\\32bit"
    #   SAPNWRFC = "C:\\SAPRFCSDK\\32bit\\sapnwrfc.dll"
    # elif str(platform.architecture()[0]) == "64bit":
    #   os.environ['PATH'] += ";C:\\SAPRFCSDK\\64bit"
    #   SAPNWRFC = "C:\\SAPRFCSDK\\64bit\\sapnwrfc.dll"
 
    SAPNWRFC = "sapnwrfc.dll"
 
    SAP = windll.LoadLibrary(SAPNWRFC)
 
    #-Prototypes------------------------------------------------------------
    SAP.RfcAppendNewRow.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcAppendNewRow.restype = c_void_p
 
    SAP.RfcCloseConnection.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcCloseConnection.restype = c_ulong
 
    SAP.RfcCreateFunction.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcCreateFunction.restype = c_void_p
 
    SAP.RfcSetInt.argtypes = [c_void_p, c_wchar_p, c_ulong, POINTER(ErrInf)]
    SAP.RfcSetInt.restype = c_ulong
 
    SAP.RfcCreateFunctionDesc.argtypes = [c_wchar_p, POINTER(ErrInf)]
    SAP.RfcCreateFunctionDesc.restype = c_void_p
 
    SAP.RfcDestroyFunction.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcDestroyFunction.restype = c_ulong
 
    SAP.RfcDestroyFunctionDesc.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcDestroyFunctionDesc.restype = c_ulong
 
    SAP.RfcGetChars.argtypes = [c_void_p, c_wchar_p, c_void_p, c_ulong, \
    POINTER(ErrInf)]
    SAP.RfcGetChars.restype = c_ulong
 
    SAP.RfcGetCurrentRow.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcGetCurrentRow.restype = c_void_p
 
    SAP.RfcGetFunctionDesc.argtypes = [c_void_p, c_wchar_p, POINTER(ErrInf)]
    SAP.RfcGetFunctionDesc.restype = c_void_p
 
    SAP.RfcCreateTable.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcCreateTable.restype = c_void_p
 
 
    SAP.RfcGetRowCount.argtypes = [c_void_p, POINTER(c_ulong), \
    POINTER(ErrInf)]
    SAP.RfcGetRowCount.restype = c_ulong
 
    SAP.RfcGetStructure.argtypes = [c_void_p, c_wchar_p, \
    POINTER(c_void_p), POINTER(ErrInf)]
    SAP.RfcGetStructure.restype = c_ulong
 
    SAP.RfcGetTable.argtypes = [c_void_p, c_wchar_p, POINTER(c_void_p), \
    POINTER(ErrInf)]
    SAP.RfcGetTable.restype = c_ulong
 
    SAP.RfcGetVersion.argtypes = [POINTER(c_ulong), POINTER(c_ulong), \
    POINTER(c_ulong)]
    SAP.RfcGetVersion.restype = c_wchar_p
 
    SAP.RfcInstallServerFunction.argtypes = [c_wchar_p, c_void_p, \
    c_void_p, POINTER(ErrInf)]
    SAP.RfcInstallServerFunction.restype = c_ulong
 
    SAP.RfcInvoke.argtypes = [c_void_p, c_void_p, POINTER(ErrInf)]
    SAP.RfcInvoke.restype = c_ulong
 
    SAP.RfcListenAndDispatch.argtypes = [c_void_p, c_ulong, POINTER(ErrInf)]
    SAP.RfcListenAndDispatch.restype = c_ulong
 
    SAP.RfcMoveToFirstRow.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcMoveToFirstRow.restype = c_ulong
 
    SAP.RfcMoveToNextRow.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcMoveToNextRow.restype = c_ulong
 
    SAP.RfcOpenConnection.argtypes = [POINTER(ConnParams), c_ulong, \
    POINTER(ErrInf)]
    SAP.RfcOpenConnection.restype = c_void_p
 
    SAP.RfcPing.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcPing.restype = c_ulong
 
    SAP.RfcRegisterServer.argtypes = [POINTER(SConParams), c_ulong, \
    POINTER(ErrInf)]
    SAP.RfcRegisterServer.restype = c_void_p
 
    SAP.RfcSetChars.argtypes = [c_void_p, c_wchar_p, c_wchar_p, c_ulong, \
    POINTER(ErrInf)]
    SAP.RfcSetChars.restype = c_ulong
 
 
 
 
 
           
   
 
    # FileName = "sapnwrfc.py"
    # exec(compile(open(FileName).read(), FileName, "exec"))
   
    #-Main------------------------------------------------------------------
   
    RfcConnParams[0].name = "ASHOST"; RfcConnParams[0].value = "34.194.191.113"
    RfcConnParams[1].name = "SYSNR" ; RfcConnParams[1].value = "01"
    RfcConnParams[2].name = "CLIENT"; RfcConnParams[2].value = "100"
    RfcConnParams[3].name = "USER"  ; RfcConnParams[3].value = "RAJKUMARS"
    RfcConnParams[4].name = "PASSWD"; RfcConnParams[4].value = "JaiHanuman10"
   
    tables = []
    res = []
    val = 50
    hRFC = SAP.RfcOpenConnection(RfcConnParams, 5, RfcErrInf)
    if hRFC != None:
   
        charBuffer = create_unicode_buffer(1048576 + 1)
        charBuffer1 = create_unicode_buffer(1048576 + 1)
   
    hFuncDesc = SAP.RfcGetFunctionDesc(hRFC, "ZTABLE_NAMES_DESC", RfcErrInf)
    if hFuncDesc != 0:
        hFunc = SAP.RfcCreateFunction(hFuncDesc, RfcErrInf)
        if hFunc != 0:
            rc = SAP.RfcSetInt(hFunc, "N",val, RfcErrInf)
            # print(SAP.RfcInvoke(hRFC, hFunc, RfcErrInf))
            if SAP.RfcInvoke(hRFC, hFunc, RfcErrInf) == RFC_OK:
       
                hTable = c_void_p(0)
                print(SAP.RfcGetTable(hFunc, "DATA", hTable, RfcErrInf))
                if SAP.RfcGetTable(hFunc, "DATA", hTable, RfcErrInf) == RFC_OK:
                    RowCount = c_ulong(0)
                rc = SAP.RfcGetRowCount(hTable, RowCount, RfcErrInf)
                print(RowCount)
                rc = SAP.RfcMoveToFirstRow(hTable, RfcErrInf)
                for i in range(0, RowCount.value):
                    hRow = SAP.RfcGetCurrentRow(hTable, RfcErrInf)
                    rc = SAP.RfcGetChars(hRow, "TAB", charBuffer, 512, RfcErrInf)
                    rc = SAP.RfcGetChars(hRow, "DESC", charBuffer1, 512, RfcErrInf)
                    # print(str(charBuffer.value))
                    # tables.append(dict(table = str(charBuffer.value).strip(),desc = str(charBuffer1.value)))
                    res.append(str(charBuffer.value) + "~" + str(charBuffer1.value)) # Print as a dictionary
                    if i < RowCount.value:
                        rc = SAP.RfcMoveToNextRow(hTable, RfcErrInf)
       
            rc = SAP.RfcDestroyFunction(hFunc, RfcErrInf)
       
        rc = SAP.RfcCloseConnection(hRFC, RfcErrInf)
 
        dd02l_desc.objects.all().delete()
 
        print("Hello Yash")
        # print(res[0][0])
        customers_to_create=[]
        for i in range(RowCount.value):
            result = res[i].split("~")
            Dd02 = dd02l_desc(
                        table = result[0],
                        description = result[1]
                    )
           
            # print(i+" "+RowCount.value)
            # break
 
            customers_to_create.append(Dd02)
            if i%1000 == 0:
                dd02l_desc.objects.bulk_create(customers_to_create, ignore_conflicts=True)
                customers_to_create = []
        # Customer.objects.all().delete()
 
        # print(tables)
        return Response(tables)
    else:
        print(RfcErrInf.key)    
        print(RfcErrInf.message)
   
    del SAP
 
 
@api_view(['GET'])
def SAPtables(request,load):
 
    print("Hello called Get Api")
    load = load * 50
    sorted_objects = dd02l_desc.objects.order_by('table')[:load]
    # projects = project_details.objects.all()
    serializer = DD02LSerializer(sorted_objects, many=True)
    return Response(serializer.data)
   
   
@api_view(['GET'])
def SAPTableSearch(request,tab):
    print("Hello called search Get Api")
 
 
      # 1. Query for starts with
    starts_with_objects = dd02l_desc.objects.filter(table__istartswith=tab)
 
    # 2. Query for contains (excluding starts with to avoid duplicates)
    contains_objects = dd02l_desc.objects.filter(
        table__icontains=tab
    ).exclude(table__istartswith=tab)  # Exclude the starts_with results
 
    # 3. Combine and order the results
    combined_objects = (starts_with_objects.annotate(order_priority=Value(0, output_field=IntegerField()))  # starts with priority 0
                        .union(contains_objects.annotate(order_priority=Value(1, output_field=IntegerField()))) # contains priority 1
                        .order_by('order_priority', 'table')) # order by priority and then table name
 
    serializer = DD02LSerializer(combined_objects, many=True)
    return Response(serializer.data)











l=[False,""]

@api_view(['POST'])
def HANAconn(request):
    print("Hana")
    print(request.data)
    try:
        conn = dbapi.connect(
            # address="10.56.7.40",
            address = request.data['host'],
            # port=30015,
            port=int(request.data['port']),
            # user="SURYAC",
            # password="Surya@2727",
            # user="SAMPATHS",
            # password="Sampath@123",
            # user="RUPAM",
            user = request.data['username'],
            password= request.data['password'],
            # password="Mrupa09$",
            encrypt='true',
            sslValidateCertificate='false'
        )
        print(conn.isconnected())
        l[0]=conn
        l[1] = request.data['username']
    #     cursor = conn.cursor()
    #     cursor.execute("SELECT TABLE_NAME FROM SYS.TABLES WHERE SCHEMA_NAME = '"+l[1]+"'")
    #     rows = cursor.fetchall()
    #     rows=list(rows)
    #     tables = [dict(var = str(row[0]).strip()) for row in rows]
 
    #     print(tables)
    #     # return Response(tables)
       
    #     return Response(tables,status=status.HTTP_200_OK)
    except:
        # return Response("failure")  
        return Response(status=status.HTTP_404_NOT_FOUND)
   
    if(conn.isconnected):  
        # return HttpResponse("success")
        return Response(status=status.HTTP_200_OK)
    # return HttpResponse("failure")
    return Response(status=status.HTTP_404_NOT_FOUND)


@api_view(['GET'])
def HANAtables(request,p_id,c_name):
    connection = Connection.objects.filter(project_id=p_id,connection_name=c_name)
    json_data = serialize('json', list(connection))
    json_data = json.loads(json_data)[0]['fields']
    print(json_data)
    conn = conn = dbapi.connect(
            # address="10.56.7.40",
            address = json_data['host'],
            # port=30015,
            port=int(json_data['port']),
            # user="SURYAC",
            # password="Surya@2727",
            # user="SAMPATHS",
            # password="Sampath@123",
            # user="RUPAM",
            user = json_data['username'],
            password= json_data['password'],
            # password="Mrupa09$",
            encrypt='true',
            sslValidateCertificate='false'
        )
    cursor = conn.cursor()
    cursor.execute("SELECT TABLE_NAME FROM SYS.TABLES WHERE SCHEMA_NAME = '"+json_data['username']+"'")
    rows = cursor.fetchall()
    rows=list(rows)
    tables = [dict(table = str(row[0]).strip(),desc="") for row in rows]
 
    print(tables)
    return Response(tables)  


@api_view(['POST'])
def ProjectCreate(request):
    print("Hello called Post")
    print(request.data)
    project = ProjectSerializer(data=request.data)

    # validating for already existing data
    # print("varun : ",Project.objects.filter(project_name=request.data['project_name']))

    if Project.objects.filter(project_name=request.data['project_name']):
        return Response(status=status.HTTP_406_NOT_ACCEPTABLE)

    if project.is_valid():
        project.save()
        # proj = project_details.objects.get(project_name=request.data['project_name'])
        # print("Id : ",proj.proj_id)
        return Response(project.data)
    else:
        return Response(status=status.HTTP_409_CONFLICT)


@api_view(['GET'])
def ProjectGet(request):
    print("Hello called Get Api")
    sorted_objects = Project.objects.order_by('-created_at')
    # projects = project_details.objects.all()
    serializer = ProjectSerializer(sorted_objects, many=True)
    return Response(serializer.data)


@api_view(['PUT'])
def projectUpdate(request,pk):
    print("Hello called update")
    print(request.data)
    project = Project.objects.get(project_id=pk)
    data = ProjectSerializer(instance=project, data=request.data)

    

    if data.is_valid():
        data.save()
        print("edjnkfhjrvfh")
        return Response(data.data)
    else:
        return Response(status=status.HTTP_404_NOT_FOUND)

@api_view(['DELETE'])
def project_delete(request,pk):
 
    # pk = request.data['project_name']
    print("Hello called Delete")
    if Project.objects.filter(project_id=pk).exists():
        project = Project.objects.get(project_id=pk)
        if project:
            serializer = ProjectSerializer(project)
            project.delete()
            return Response(serializer.data,status=status.HTTP_202_ACCEPTED)
        else:
            return Response(status=status.HTTP_404_NOT_FOUND)
    else:
        return Response(status=status.HTTP_404_NOT_FOUND)



@api_view(['POST'])
def ConnectionCreate(request):
    # request.data['connection_type']=""
    data = request.data
    data['connection_type'] = data['connection_type'].upper()
    connection = ConnectionSerializer(data=data)
    print("Hello post connection called")
    if Connection.objects.filter(project_id=request.data["project_id"],connection_name = request.data["connection_name"]).exists():
        return Response(status=status.HTTP_406_NOT_ACCEPTABLE)
    if connection.is_valid():
        
        connection.save()
        return Response(connection.data,status=status.HTTP_201_CREATED)
    else:
        return Response(status=status.HTTP_409_CONFLICT)
    
@api_view(['GET'])
def ConnectionGet(request):
    print("hii")
    connections = Connection.objects.all()
    serializer = ConnectionSerializer(connections,many=True)
    return Response(serializer.data) 

@api_view(['PUT'])
def ConnectionUpdate(request,p_id,c_name):
    print(request.data)
    print(p_id,c_name)
    connection = Connection.objects.get(project_id=p_id,connection_name=c_name)
    data = ConnectionSerializer(instance=connection, data=request.data)
    if data.is_valid():
        print("jfnjkjefkjfkjnrkj")
        data.save()
        return Response(data.data,status=status.HTTP_202_ACCEPTED)
    else:
        print("ffffffffffffffffffffffffffffffffffffff")
        return Response(status=status.HTTP_404_NOT_FOUND)
    
@api_view(['DELETE'])
def connectionDelete(request,p_id,c_name):
    if Connection.objects.filter(project_id=p_id,connection_name=c_name).exists():
        connection = Connection.objects.get(project_id=p_id,connection_name=c_name)
        if connection:
            connection.delete()
            print("ssssssuccesssss")
            return Response(c_name,status=status.HTTP_202_ACCEPTED)
        else:
            return Response(status=status.HTTP_404_NOT_FOUND)
    else:
        return Response(status=status.HTTP_404_NOT_FOUND)   

@api_view(['GET'])
def ConnectionGetSingle(request,p_id,c_name):
    if Connection.objects.filter(project_id=p_id,connection_name=c_name).exists():
        connection = Connection.objects.get(project_id=p_id,connection_name=c_name)
        if connection:
            serializer = ConnectionSerializer(connection)
            return Response(serializer.data)
        else:
            return Response(status=status.HTTP_404_NOT_FOUND)
    else:
        return Response(status=status.HTTP_404_NOT_FOUND)   
    
@api_view(['GET'])
def ProjectGetSingle(request,p_id):
    if Project.objects.filter(project_id=p_id).exists():
        project = Project.objects.get(project_id=p_id)
        if project:
            serializer = ProjectSerializer(project)
            return Response(serializer.data)
        else:
            return Response(status=status.HTTP_404_NOT_FOUND)
    else:
        return Response(status=status.HTTP_404_NOT_FOUND)   


@api_view(['PUT'])
def connectionRename(request,re_val,p_id,c_name):
    # print(request.data)
    connection = Connection.objects.get(project_id=p_id,connection_name=c_name)
    data = ConnectionSerializer(instance=connection, data=request.data)
    request.data['connection_name'] = re_val
    data = ConnectionSerializer(instance=connection, data=request.data)
    if data.is_valid():
        try:
            data.save()
            return  Response(c_name,status=status.HTTP_202_ACCEPTED)
        except:
            return Response(re_val,status=status.HTTP_404_NOT_FOUND)
    else:
        print(data.errors)
        return Response(re_val,status=status.HTTP_404_NOT_FOUND)
    
    
        


# @api_view(['GET'])  # This function is for dynamically creating tables and you have to pass
def create_table(table_name,columns):
 
    try:
        with connection.cursor() as cursor:
                # 1. Check if the table exists

                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
                table_exists = cursor.fetchone() is not None
 
                if not table_exists:
                    create_table_sql = f"CREATE TABLE {table_name} ("
                    for col_name, col_type in columns:
                        # valid_col_name = col_name.replace("/", "_")
                        # create_table_sql += f"{col_name} {col_type},"
                        create_table_sql += f"\"{col_name}\" {col_type},"
                    create_table_sql = create_table_sql[:-1] + ")"
                    print(create_table_sql)
                    # with transaction.atomic(using='default'):  
                    cursor.execute(create_table_sql)
                    print(f"Table '{table_name}' created.")
 
    except Exception as e:
        print(f"Error creating table: {e}")
        connection.rollback()
        # return Response(f"Error creating/inserting data: {e}", status=500)
 
 

 
@api_view(['GET'])
def viewDynamic(request):
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]  # Extract table names
            return Response(tables)
 
 
        #     table_name = "bala7"
        #     cursor.execute(f"SELECT * FROM {table_name}")
        #     rows = cursor.fetchall()
 
        # # Print the data (or process it as needed)
        # ans = []
        # for row in rows:
        #     ans.append(row)
        # return Response(ans)
 
 
 
            # table_name = "bala5"
            #   # Method 1: Using PRAGMA table_info (recommended)
            # cursor.execute(f"PRAGMA table_info({table_name});")
            # columns_info = cursor.fetchall()
            # for column in columns_info:
            #     print(column)  # Print all column details
            #     print(f"Column Name: {column[1]}")
            # return Response("Hello")
 
    except Exception as e:
        print(f"Error creating/checking table: {e}")  # Print the error to the console
        connection.rollback()  # Rollback any partial changes on error
        return Response(f"Error creating/checking table: {e}", status=500)
   
 
 
def deleteSqlLiteTable(table_name):
 
    # table_name = "demo"
 
    try:
            with connection.cursor() as cursor:
                # Use parameterized query to prevent SQL injection
                cursor.execute(f"DROP TABLE IF EXISTS  {table_name}") # Correct: parameterized query
                # or cursor.execute(f"DROP TABLE IF EXISTS {table_name}") # Less secure way
                print(f"Table '{table_name}' dropped (IF EXISTS).")
    except Exception as e:
            print(f"Error dropping table '{table_name}': {e}")
   
 
 
   
 
    return Response("Hii")
 
    # columns = [
    #     ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
    #     ("productname", "TEXT")
    #     ("price", "REAL"),
    #     ("description", "TEXT"),
    #     ("is_active", "BOOLEAN"),
    #     ("created_at", "DATETIME"),
    # ]
 
    # table_name = "bala8"
 
    # create_table(columns,table_name)
 
    # data_to_insert =
    # [
    #     {
    #         "productname": "varun A"
    #         "price": 10.99,
    #         "description": "Product A description",
    #         "is_active": True,
    #         "created_at": "2024-10-29 17:00:00",
    #     },
    #     {
    #         "productname": "Product B"
    #         "price": 20.00,
    #         "description": "Product B description",
    #         "is_active": False,
    #         "created_at": "2024-10-29 18:00:00",
    #     }
    # ]
 
 
def TableName_Modification(text):
    
    allowed_chars = string.ascii_letters + string.digits + ' '  # Add space if needed
 
    # Filter out characters not in the allowed set
    cleaned_text = ''.join(char for char in text if char in allowed_chars)
    
    return re.sub(r'\s+', '_', cleaned_text)
 


def sheet_get(df,sheet_data,obj_id):
 
 
    # deleteSqlLiteTable()
 
    project_id = sheet_data['project_id']
    obj_name = sheet_data['obj_name']
    template_name  = sheet_data['template_name']
    target_tables =[]
 
    # sheet = "Field List"
    # df1 = pd.read_excel(r"C:\Users\varunbalaji.gada\Downloads\excel_dmc.xls",engine="xlrd", sheet_name=sheet)
    # template_name=df1.columns[0]
    # print(template_name)
 
    # sheet = "Field List"
    # df = pd.read_excel(r"C:\Users\varunbalaji.gada\Downloads\excel_dmc.xls",engine="xlrd", sheet_name=sheet,skiprows=[0,1,2],na_filter=False)
    # print(df)
 
    # obj_save = {
    #     "project_id" : project_id,
    #     "obj_name" : obj_name,
    #     "template_name" : template_name
    # }
    # obj = ObjectSerializer(data=obj_save)
    # if obj.is_valid():
    #     obj_instance = obj.save()
    #     obj_id = obj_instance.obj_id
    #     # return Response(obj_id)
    # else:
    #     print("Error at Object saving")
    #     return "Error at Object saving"
 
    x=0
    columns = []
    # segment = "Additional Descriptions"
    group = ""
    customers_to_create=[]
    field_data = []
    for ind,i in df.iterrows():
        col = []
        data = []
        # print(i['Sheet Name'] , " : " , i['Sheet Name']!="" and i['Sheet Name'] == segment)
        if i['Sheet Name']=="":
 
            if i['SAP Field'] !="":
                col.append(i['SAP Field'])
                data.append(i['SAP Field'])
                if i['Type'].lower() == 'text':
                    col.append("TEXT")
                elif i['Type'].lower() == 'Number':
                    col.append("INTEGER")
                elif i['Type'].lower() == 'date':
                    col.append("DATE")
                elif i['Type'].lower() == 'boolean':
                    col.append("BOOLEAN")
                elif i['Type'].lower() == 'datetime':
                    col.append("DATETIME")
                else:
                    col.append("TEXT")
                columns.append(col)
                data.append(i['Field Description'])
                if i['Importance'] != "":
                    data.append("True")
                else:
                    data.append("False")
                data.append(i['SAP Structure'])
                if(i['Group Name']=="Key"):
                    data.append("True")
                    group = "Key"
                elif i['Group Name'] != "":
                    group = i['Group Name']
                    data.append("False")
                elif i['Group Name'] == "":
                    if group == "Key":
                        data.append("True")
                    else:
                        data.append("False")
 
                field_data.append(data)
        else:
            # print("Columns varun : ",len(columns))
            if len(columns) == 0:
                seg_name =TableName_Modification(i['Sheet Name'])
                tab ="t"+"_"+project_id+"_"+str(obj_name)+"_"+str(seg_name)
                seg = i['Sheet Name']
                seg_obj = {
                    "project_id" : project_id,
                    "obj_id" : obj_id,
                    "segement_name":seg,
                    "table_name" : tab
                }
                target_tables.append(tab)
                seg_instance = SegementSerializer(data=seg_obj)
                if seg_instance.is_valid():
                    seg_id_get = seg_instance.save()
                    segment_id = seg_id_get.segment_id
 
                else:
                    return Response("Error at first segement creation")
            if len(columns) != 0:
                   
 
                create_table(tab,columns)
               
 
                for d in field_data:
                    field_obj = {
                        "project_id" : project_id,
                        "obj_id" : obj_id,
                        "segement_id" : segment_id,
                        "sap_structure" : d[3],
                        "fields" : d[0],
                        "description" : d[1],
                        "isMandatory" : d[2],
                        "isKey" : d[4]
                    }
                    field_instance = FieldSerializer(data=field_obj)
                    if field_instance.is_valid():
                        field_id_get = field_instance.save()
                        field_id = field_id_get.field_id
                    else:
                        return Response("Error at Field Creation")
 
 
                seg = i['Sheet Name']
                seg_name = TableName_Modification(i['Sheet Name'])
                tab ="t"+"_"+project_id+"_"+str(obj_name)+"_"+str(seg_name)
                seg_obj = {
                    "project_id" : project_id,
                    "obj_id" : obj_id,
                    "segement_name":seg,
                    "table_name" : tab
                }
                target_tables.append(tab)
                # break
                seg_instance = SegementSerializer(data=seg_obj)
                if seg_instance.is_valid():
                    seg_id_get = seg_instance.save()
                    segment_id = seg_id_get.segment_id
 
                   
                else:
                    return Response("Error at segement creation")
                columns=[]
                field_data=[]
    create_table(tab,columns)
    for d in field_data:
        field_obj = {
            "project_id" : project_id,
            "obj_id" : obj_id,
            "segement_id" : segment_id,
            "sap_structure" : d[3],
            "fields" : d[0],
            "description" : d[1],
            "isMandatory" : d[2],
            "isKey" : d[4]
        }
        field_instance = FieldSerializer(data=field_obj)
        if field_instance.is_valid():
            field_id_get = field_instance.save()
            field_id = field_id_get.field_id
        else:
            return Response("Error at Field Creation")
    return target_tables
 
 
 

@api_view(['GET'])
def project_dataObject(request,pid,ptype):

    try:
        print(ptype)
        print("Hello called object Get Api")
        obj = objects.objects.all()
        objs = []
        for item in obj:
            # obje
            print(item.obj_id)
            szr = ObjectSerializer(item)
            projId=szr.data['project_id']
            if Project.objects.filter(project_id = projId):
                prj = Project.objects.get(project_id = projId)
                pType = prj.project_type
                pName = prj.project_name
                # print(pType)
                if(ptype==pType):
                    obj_dict = model_to_dict(item)
                    obj_dict['project_name'] = pName
                    objs.append(obj_dict)

        print(objs)
        return Response(objs)
    except Exception as e:
        print(f"Error in project_dataObject: {e}")  # Log the error for debugging
        return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)
   
 
@api_view(['GET'])
def DataObject_Segements(request,pid,oid):
 
    connections = segments.objects.filter(project_id=pid,obj_id=oid)
    if connections:
        serializer = SegementSerializer(connections,many=True)
        return Response(serializer.data)
    else:
        return Response(status=status.HTTP_404_NOT_FOUND)
   
 
 
@api_view(['GET'])
def Segements_Fields(request,pid,oid,sid):
 
    connections = fields.objects.filter(project_id=pid,obj_id=oid,segement_id=sid)
    if connections:
        serializer = FieldSerializer(connections,many=True)
        print(serializer.data)
        return Response(serializer.data)
    else:
        return Response(status=status.HTTP_404_NOT_FOUND)
 


 
@api_view(['POST'])
def objects_create(request):
    print("Hello called Objects Post")
    file = request.FILES['file']
    obj_name = request.data['obj_name']
    project_id = request.data['project_id']
    template_name = request.data['template_name']
    ob_name = obj_name.strip()  
 
    obj_data = {
        "obj_name" : ob_name,
        "project_id" : project_id,
        "template_name" : template_name
    }
    print(obj_data)
    print("Heloooooooooooooo")
    obj = ObjectSerializer(data=obj_data)
 
   
    if objects.objects.filter(project_id=obj_data['project_id'],obj_name = obj_data['obj_name']):
        return Response(status=status.HTTP_406_NOT_ACCEPTABLE)
 
    if obj.is_valid():
        obj_instance=obj.save()
        objid = obj_instance.obj_id
 
        df = pd.read_excel(file,sheet_name="Field List",skiprows=[0,1,2],na_filter=False)
        print(df)
        target_tables = sheet_get(df,obj_data,objid)
 
        print("Target tables are : ",len(target_tables))
        excel_file = pd.ExcelFile(file)
         # Get the sheet names
        sheet_names = excel_file.sheet_names
        print("No. of Sheets are : ",len(sheet_names))
        # return Response(obj.data)
 
 
        i = 0
        j = 0
        skip_count = 2  # Number of iterations to skip
 
        for sn in sheet_names:
            if i < skip_count:
                i += 1
                continue
            table_name = target_tables[j]
            j+=1
            df1 = pd.read_excel(file,sheet_name=sn,skiprows=[0,1,2,3,5,6,7],na_filter=False)
            insert_data_from_dataframe(df1,table_name)
 
 
 
 
 
 
        return Response(obj.data)
    else:
        print(obj.error_messages)
        return Response(status=status.HTTP_409_CONFLICT)
 
    return Response("Hello")
 
 
@api_view(['PUT'])
def objects_update(request,oid):
 
    print("Hello called objects update")
    # return Response("Hello")
    # print(request.data)
   
 
    file = request.FILES['file']
    obj_name = request.data['obj_name']
    project_id = request.data['project_id']
    template_name = request.data['file_name']
 
    obj_data = {
        "obj_name" : obj_name,
        "project_id" : project_id,
        "template_name" : template_name
    }
 
    if objects.objects.filter(obj_id=oid).exists():
        obj = objects.objects.get(obj_id=oid)
        if obj.obj_name == obj_name:
 
            if obj:
               
                #Deleting existing segements and tables
                seg = segments.objects.filter(project_id=obj.project_id,obj_id=oid)
                for s in seg:
                    deleteSqlLiteTable(s.table_name)
                    # segSerializer = SegementSerializer(s)
                    # s.delete()
 
 
                #Creating new excel tables and details into segements and fields tables
                data = ObjectSerializer(instance=obj, data=obj_data)
                if data.is_valid():
                    obj_instance=data.save()
                    objid = obj_instance.obj_id
 
                    df = pd.read_excel(file,sheet_name="Field List",skiprows=[0,1,2],na_filter=False)
                    # print(df)
                    sheet_delete(df,obj_data,objid)
                    target_tables = sheet_update(df,obj_data,objid)
 
                    print("Target tables are : ",len(target_tables))
                    excel_file = pd.ExcelFile(file)
                    # Get the sheet names
                    sheet_names = excel_file.sheet_names
                    print("No. of Sheets are : ",len(sheet_names))
                    # return Response(obj.data)
 
 
                    i = 0
                    j = 0
                    skip_count = 2  # Number of iterations to skip
 
                    for sn in sheet_names:
                        if i < skip_count:
                            i += 1
                            continue
                        table_name = target_tables[j]
                        j+=1
                        df1 = pd.read_excel(file,sheet_name=sn,skiprows=[0,1,2,3,5,6,7],na_filter=False)
                        insert_data_from_dataframe(df1,table_name)
 
                    return Response(data.data)
                else:
                    return Response(status=status.HTTP_406_NOT_ACCEPTABLE)
            else:
                return Response(status=status.HTTP_404_NOT_FOUND)
 
 
 
                # return Response(serializer.data,status=status.HTTP_202_ACCEPTED)
        else:
                return Response(status=status.HTTP_401_UNAUTHORIZED)
    else:
        return Response(status=status.HTTP_404_NOT_FOUND)
 
 
 


 


@api_view(['DELETE'])
def objects_delete(request,oid):
    print("Hello called object Delete")
    if objects.objects.filter(obj_id=oid).exists():
        obj = objects.objects.get(obj_id=oid)
        if obj:
 
            seg = segments.objects.filter(project_id=obj.project_id,obj_id=oid)
            for s in seg:
                deleteSqlLiteTable(s.table_name)
            serializer = ObjectSerializer(obj)
            obj.delete()
            return Response(serializer.data,status=status.HTTP_202_ACCEPTED)
        else:
            return Response(status=status.HTTP_404_NOT_FOUND)
    else:
        return Response(status=status.HTTP_404_NOT_FOUND)
 
 
@api_view(['GET'])
def objects_get(request,oid):
    print("Hello called object Get Api")
    obj = objects.objects.get(obj_id=oid)
    if obj:
        serializer = ObjectSerializer(obj)
        print(serializer.data['project_id'])   
        return Response(serializer.data)
    else:
        return Response(status=status.HTTP_404_NOT_FOUND)
 

 
@api_view(['POST'])
def xls_read(request):
    file = request.FILES['file']
    excel_file = pd.ExcelFile(file)
    # Get the sheet names
    sheet_names = excel_file.sheet_names
    # Print the sheet names
    print(len(sheet_names))
    print(sheet_names)
    if len(sheet_names) <= 1 :
        return Response(status=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE)
    else:
        if 'Field List' in sheet_names:
            # print("Yes Iam in ...")
            df = pd.read_excel(file, sheet_name='Field List')
           
            val = df.columns[0].split(':')
            return Response(val[1])
        else:
            return Response(status=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE)
 
 
@api_view(['GET'])
def tableDelete(request):
 
    # lst = ['demo0','demo138','demo143','demo154','demo171','demo177','demo185','demo193','demo201'
    #        ,'demo206','demo218','demo227','demo278','demo290','demo312','demo321','demo496','demo490'
    #        ,'demo496','demo521','demo553','demo561','demo618','demo644','demo656','demo698']
   
    # for l in lst:
    deleteSqlLiteTable('demo469')
    return Response("Hello Deleted")



def insert_data_from_dataframe(dataframe, table_name, database_name='default'):
    print(dataframe)
    print(dataframe.columns)
    try:
        with connections[database_name].cursor() as cursor:
            for index, row in dataframe.iterrows():
                # Construct the INSERT INTO statement
                # column_names = ', '.join(dataframe.columns)
                quoted_column_names = ', '.join(f'"{col}"' for col in dataframe.columns)
                column_names = quoted_column_names
                placeholders = ', '.join(['%s'] * len(dataframe.columns))
                insert_sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders});"
 
                # Execute the INSERT statement with data from the row
                cursor.execute(insert_sql, tuple(row))
 
            # Commit the changes within a transaction
            with transaction.atomic(using=database_name):
                cursor.execute("COMMIT;")
 
        print(f"Data inserted successfully into '{table_name}' in {database_name} database.")
       
 
    except Exception as e:
        print(f"Error inserting data: {e}")


def create_table_dynamically(table_name, fields, database_name='default'):
    try:
        with connections[database_name].cursor() as cursor:
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            if cursor.fetchone():
                print(f"Table '{table_name}' already exists in {database_name} database.")
                return  
            create_table_sql = f"CREATE TABLE {table_name} ("
            for field_name, field_type in fields.items():
                create_table_sql += f"{field_name} {field_type},"
            create_table_sql = create_table_sql[:-1] + ");"
            print(create_table_sql)
            with transaction.atomic(using=database_name):  
                cursor.execute(create_table_sql)
                return 1
            print(f"Table '{table_name}' created successfully in {database_name} database.")
 
    except sqlite3.Error as e:
        print(f"SQLite3 Error: {e}")
    except Exception as e:
        print(f"Error creating table: {e}")
 
def convert_list_to_fields(field_list):
    field_dict = {}
    for field_name, field_type in field_list:
        if field_type.lower() == 'text':
            field_dict[field_name] = 'TEXT'
        elif field_type.lower() == 'date':
            field_dict[field_name] = 'DATE'
        elif field_type.lower() == 'integer':
            field_dict[field_name] = 'INTEGER'
        elif field_type.lower() == 'real':
            field_dict[field_name] = 'REAL'
        elif field_type.lower() == 'boolean':
            field_dict[field_name] = 'BOOLEAN'
        elif field_type.lower() == 'datetime':
            field_dict[field_name] = 'DATETIME'
        else:
            field_dict[field_name] = 'TEXT'  
    return field_dict
 
def drop_table_dynamically(table_name, database_name='default'):
    try:
        with connections[database_name].cursor() as cursor:
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            if not cursor.fetchone():
                print(f"Table '{table_name}' does not exist in {database_name} database.")
                return
 
            drop_table_sql = f"DROP TABLE {table_name};"
 
            with transaction.atomic(using=database_name):
                cursor.execute(drop_table_sql)
                return 1
 
            print(f"Table '{table_name}' dropped successfully from {database_name} database.")
 
    except Exception as e:
        print(f"Error dropping table: {e}")
       
@api_view(['POST'])
def fileCreate(request):
    # request.data['connection_type']=""
    connection = FileSerializer(data=request.data)
    print("Hello post file called")
    if FileConnection.objects.filter(project_id=request.data["project_id"],fileName = request.data["fileName"]).exists():
        return Response(status=status.HTTP_406_NOT_ACCEPTABLE)
    if connection.is_valid():
       
        connection.save()
        return Response(connection.data,status=status.HTTP_201_CREATED)
    else:
        return Response(status=status.HTTP_409_CONFLICT)
   
@api_view(['GET'])
def fileGet(request):
    print("hii")
    connections = FileConnection.objects.all()
    serializer = FileSerializer(connections,many=True)
    return Response(serializer.data)
 
@api_view(['PUT'])
def fileUpdate(request,p_id,f_name):
    print(request.data)
    print(p_id,f_name)
    connection = FileConnection.objects.get(project_id=p_id,fileName=f_name)
    data = FileSerializer(instance=connection, data=request.data)
    if data.is_valid():
        print("jfnjkjefkjfkjnrkj")
        data.save()
        return Response(data.data,status=status.HTTP_202_ACCEPTED)
    else:
        print("ffffffffffffffffffffffffffffffffffffff")
        return Response(status=status.HTTP_404_NOT_FOUND)
   
@api_view(['DELETE'])
def fileDelete(request,p_id,f_name):
    if FileConnection.objects.filter(project_id=p_id,fileName=f_name).exists():
        connection = FileConnection.objects.get(project_id=p_id,fileName=f_name)
        if connection:
            connection.delete()
            print("ssssssuccesssss")
            return Response(f_name,status=status.HTTP_200_OK)
        else:
            return Response(status=status.HTTP_404_NOT_FOUND)
    else:
        return Response(status=status.HTTP_404_NOT_FOUND)  
 
@api_view(['GET'])
def fileGetSingle(request,p_id,f_name):
    if FileConnection.objects.filter(project_id=p_id,fileName=f_name).exists():
        connection = FileConnection.objects.get(project_id=p_id,fileName=f_name)
        if connection:
            serializer = FileSerializer(connection)
            return Response(serializer.data)
        else:
            return Response(status=status.HTTP_404_NOT_FOUND)
    else:
        return Response(status=status.HTTP_404_NOT_FOUND)  
 
@api_view(['PUT'])
def fileRename(request,re_val,p_id,f_name):
    # print(request.data)
    connection = FileConnection.objects.get(project_id=p_id,fileName=f_name)
    request.data['fileName'] = re_val
    d={}
    d['project_id'] = request.data['project_id']
    d['fileName'] = re_val
    d['tableName'] = request.data['table_name']
    d['fileType'] = request.data['file_type']
    d['sheet'] = request.data['sheet']
    data = FileSerializer(instance=connection, data=d)
    print(data)
    if data.is_valid():
        try:
            data.save()
            return  Response(f_name,status=status.HTTP_200_OK)
        except:
            return Response(re_val,status=status.HTTP_404_NOT_FOUND)
    else:
        return Response(re_val,status=status.HTTP_404_NOT_FOUND)
 
 
 
 
class GetXL(APIView):
    def post(self, request):
        file = request.FILES['file']
        excel_file = pd.ExcelFile(file)
        # Get the sheet names
        sheet_names = excel_file.sheet_names
        # Print the sheet names
        print(sheet_names)
        # data = pd.read_excel(file)
        # data = pd.DataFrame(data)
        # column_names_list = data.columns.tolist()
        d = []
        for i in sheet_names:
            d.append(i)
        print(d)
        return Response(d)
 
class GetXLSheet(APIView):
    def post(self, request):
        data = request.data.copy()  # Create a mutable copy of request.data
 
        # Ensure project_id is an integer
        try:
            project_id = data.get('projectID')
        except (ValueError, TypeError):
            return Response({"projectID": ["Must be a valid integer."]}, status=status.HTTP_400_BAD_REQUEST)
 
        # Assign int value to project_id, where FileSerializer can get this field name
        data['project_id'] = project_id
        data['fileType'] = 'Excel'  # Set fileType directly in the data
        print(data)
        serializer = FileSerializer(data=data)
 
        if not serializer.is_valid():
            print(serializer.errors)  # Very important for debugging
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
       
        serializer.save()
        print(request.data.get('sheet'))
        df = pd.read_excel(data['file'], sheet_name = data['sheet'])
        df = pd.DataFrame(df)
        columns = list(df.columns)
        feilds= {}
        for i in columns:
            feilds[i] = "TEXT"
        tablename=request.data['tableName']
        flag = drop_table_dynamically(str(tablename))
        print(flag)
        flag = create_table_dynamically(str(tablename),feilds,"default")
        print(flag)
        insert_data_from_dataframe(dataframe=df,table_name=tablename,database_name='default')
        return Response()
 
class GetTXT(APIView):
    def post(self, request):
        file = request.FILES['file']
        delim = request.data.get('delimiter')
        data = request.data.copy()
        try:
            project_id = data.get('projectID')
        except (ValueError, TypeError):
            return Response({"projectID": ["Must be a valid integer."]}, status=status.HTTP_400_BAD_REQUEST)
 
        # Assign int value to project_id, where FileSerializer can get this field name
        data['project_id'] = project_id
        data['fileType'] = 'Text'  # Set fileType directly in the data
        print(data)
        serializer = FileSerializer(data=data)
 
        if not serializer.is_valid():
            print(serializer.errors)  # Very important for debugging
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
       
        serializer.save()
        print(delim)
        data = pd.read_table(file)
        print(data)
        # df = pd.read_excel(data['file'], sheet_name = data['sheet'])
        df = pd.DataFrame(data)
        columns = list(df.columns)
        n, flag = len(columns), 0
        for i in columns:
            if ':' in i:
                flag = 1
                break
        if flag:
            columns = []
            for i in range(n):
                s = 'Column' + str(i)
                columns.append(s)
        print(columns)
        feilds= {}
        for i in columns:
            feilds[i] = "TEXT"
        tablename=request.data['tableName']
        flag = drop_table_dynamically(str(tablename))
        print(flag)
        flag = create_table_dynamically(str(tablename),feilds,"default")
        print(flag)
        insert_data_from_dataframe(dataframe=df,table_name=tablename,database_name='default')
        return Response()
   
class GetFile(APIView):
    def post(self, request):
        data = request.data.copy()  # Create a mutable copy of request.data
 
        # Ensure project_id is an integer
        try:
            project_id = data.get('projectID')
        except (ValueError, TypeError):
            return Response({"projectID": ["Must be a valid integer."]}, status=status.HTTP_400_BAD_REQUEST)
 
        # Assign int value to project_id, where FileSerializer can get this field name
        data['project_id'] = project_id
        data['fileType'] = 'CSV'  # Set fileType directly in the data
        print(data)
        serializer = FileSerializer(data=data)
 
        if not serializer.is_valid():
            print(serializer.errors)  # Very important for debugging
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
       
        serializer.save()
        file = request.FILES['file']
        data = pd.read_csv(file)
        columns = list(data.columns)
        print(*columns, sep = ', ')
        data = pd.DataFrame(data)
 
        df = pd.DataFrame(data)
        columns = list(df.columns)
        feilds= {}
        for i in columns:
            feilds[i] = "TEXT"
        tablename=request.data['tableName']
        flag = drop_table_dynamically(str(tablename))
        print(flag)
        flag = create_table_dynamically(str(tablename),feilds,"default")
        print(flag)
        insert_data_from_dataframe(dataframe=df,table_name=tablename,database_name='default')
        return Response()
 


def sheet_update(df,sheet_data,obj_id):
    project_id = sheet_data['project_id']
    obj_name = sheet_data['obj_name']
    template_name  = sheet_data['template_name']
   
    target_tables = []
 
    x=0
    is_seg=0
    columns = []
    # segment = "Additional Descriptions"
    group = ""
    customers_to_create=[]
    field_data = []
    for ind,i in df.iterrows():
        col = []
        data = []
        # print(i['Sheet Name'] , " : " , i['Sheet Name']!="" and i['Sheet Name'] == segment)
        if i['Sheet Name']=="":
 
            if i['SAP Field'] !="":
                col.append(i['SAP Field'])
                data.append(i['SAP Field'])
                if i['Type'].lower() == 'text':
                    col.append("TEXT")
                elif i['Type'].lower() == 'Number':
                    col.append("INTEGER")
                elif i['Type'].lower() == 'date':
                    col.append("DATE")
                elif i['Type'].lower() == 'boolean':
                    col.append("BOOLEAN")
                elif i['Type'].lower() == 'datetime':
                    col.append("DATETIME")
                else:
                    col.append("TEXT")
                columns.append(col)
                data.append(i['Field Description'])
                if i['Importance'] != "":
                    data.append("True")
                else:
                    data.append("False")
                data.append(i['SAP Structure'])
                if(i['Group Name']=="Key"):
                    data.append("True")
                    group = "Key"
                elif i['Group Name'] != "":
                    group = i['Group Name']
                    data.append("False")
                elif i['Group Name'] == "":
                    if group == "Key":
                        data.append("True")
                    else:
                        data.append("False")
                field_data.append(data)
        else:
            # print("Columns varun : ",len(columns))
            if len(columns) == 0:
                seg_name =TableName_Modification(i['Sheet Name'])
                tab ="t"+"_"+project_id+"_"+str(obj_name)+"_"+str(seg_name)
                seg = i['Sheet Name']
                seg_obj = {
                    "project_id" : project_id,
                    "obj_id" : obj_id,
                    "segement_name":seg,
                    "table_name" : tab
                }
                target_tables.append(tab)
                seg_instance = segments.objects.filter(project_id=project_id,obj_id=obj_id)
                x=0
                for s in seg_instance:
                    if s.segement_name == seg:
                        x=1
                        is_seg=0
                        break
                if x==0:
                    seg_instanc = SegementSerializer(data=seg_obj)
                    if seg_instance.is_valid():
                        seg_id_get = seg_instanc.save()
                        segment_id = seg_id_get.segment_id
                        is_seg=1
                    else:
                        return Response("Error at first segement creation")
                else:
                    segment_id = s.segment_id
            if len(columns) != 0:
                   
 
                create_table(tab,columns)
 
                field_names = []
                for fie in columns:
                    field_names.append(fie[0])
               
                fields_in_table = fields.objects.filter(project_id=project_id,obj_id=obj_id,segement_id=segment_id)
               
                for v in fields_in_table:
                    if v.fields in field_names:
                        pass
                    else:
                        serlzr = FieldSerializer(v)
                        v.delete()
               
 
                # if is_seg == 1:
                for d in field_data:
                    field_obj = {
                        "project_id" : project_id,
                        "obj_id" : obj_id,
                        "segement_id" : segment_id,
                        "sap_structure" : d[3],
                        "fields" : d[0],
                        "description" : d[1],
                        "isMandatory" : d[2],
                        "isKey" : d[4]
                    }
                    field_check = fields.objects.filter(project_id=project_id,obj_id=obj_id,segement_id=segment_id)
                    y=0
                    for f in field_check:
                        if f.fields == d[0]:
                            y=1
                            break
                    if y==0:
                        field_instance = FieldSerializer(data=field_obj)
                        if field_instance.is_valid():
                            field_id_get = field_instance.save()
                            field_id = field_id_get.field_id
                        else:
                            return Response("Error at Field Creation")
                    else:
                        field_obj = {
                            "field_id" : f.field_id,
                            "project_id" : project_id,
                            "obj_id" : obj_id,
                            "segement_id" : segment_id,
                            "sap_structure" : d[3],
                            "fields" : d[0],
                            "description" : d[1],
                            "isMandatory" : d[2],
                            "isKey" : d[4]
                        }
                        field = fields.objects.get(field_id=f.field_id)
                        data = FieldSerializer(instance=field, data=field_obj)
                        # print("Fields : ",field_obj)
                        # print(data)
                        if data.is_valid():
                            # print("Valid field")
                            # print(data)
                            data.save()
                        else:
                            # print("Error : ",data.error_messages)
                            return Response("Error at Field Creation")
 
 
                field_inst = Rule.objects.filter(project_id=project_id,object_id=obj_id,segment_id=segment_id)
                if field_inst:
                    latest_version = Rule.objects.filter(
                        project_id=project_id,  # Assuming all items have the same IDs
                        object_id=obj_id,
                        segment_id=segment_id
                    ).order_by('-version_id').first()
                    field_inst = fields.objects.filter(project_id=project_id,obj_id=obj_id,segement_id=segment_id)
                    for fi in field_inst:
                        fields_tab = Rule.objects.filter(project_id=project_id,object_id=obj_id,segment_id=segment_id,version_id=latest_version.version_id,field_id=fi.field_id).first()
                        if fields_tab:
                            rule = {
                                "project_id" : project_id,
                                "object_id" : obj_id,
                                "segment_id" : segment_id,
                                "field_id" : fi.field_id,
                                "version_id" : latest_version.version_id+1,
                                "target_sap_table" : fi.sap_structure,
                                "target_sap_field" : fi.fields,
                                "source_table" : fields_tab.source_table,
                                "source_field_name" : fields_tab.source_field_name,
                                "data_mapping_rules": fields_tab.data_mapping_rules,
                                "text_description" : fi.description,
                                "isMandatory" :fi.isMandatory
                            }
                            sezr = RuleSerializer(data=rule)
                            if sezr.is_valid():
                                sezr.save()
                        else:
                            rule = {
                                "project_id" : project_id,
                                "object_id" : obj_id,
                                "segment_id" : segment_id,
                                "field_id" : fi.field_id,
                                "version_id" : latest_version.version_id+1,
                                "target_sap_table" : fi.sap_structure,
                                "target_sap_field" : fi.fields,
                                "text_description" : fi.description,
                                "isMandatory" :fi.isMandatory
                            }
                            sezr = RuleSerializer(data=rule)
                            if sezr.is_valid():
                                sezr.save()      
 
 
                seg = i['Sheet Name']
                seg_name = TableName_Modification(i['Sheet Name'])
                tab ="t"+"_"+project_id+"_"+str(obj_name)+"_"+str(seg_name)
                seg_obj = {
                    "project_id" : project_id,
                    "obj_id" : obj_id,
                    "segement_name":seg,
                    "table_name" : tab
                }
                target_tables.append(tab)
                # break
 
                seg_instance = segments.objects.filter(project_id=project_id,obj_id=obj_id)
                x=0
                for s in seg_instance:
                    if s.segement_name == seg:
                        x=1
                        is_seg=0
                        break
                if x==0:
                    seg_instanc = SegementSerializer(data=seg_obj)
                    if seg_instanc.is_valid():
                        seg_id_get = seg_instanc.save()
                        segment_id = seg_id_get.segment_id
                        is_seg=1
 
                    else:
                        return Response("Error at first segement creation")
                else:
                    segment_id = s.segment_id
 
                columns=[]
                field_data=[]
    create_table(tab,columns)
    # if is_seg==1:
 
    field_names = []
    for fie in columns:
        field_names.append(fie[0])
   
    fields_in_table = fields.objects.filter(project_id=project_id,obj_id=obj_id,segement_id=segment_id)
   
    for v in fields_in_table:
        if v.fields in field_names:
            pass
        else:
            serlzr = FieldSerializer(v)
            v.delete()
 
 
    for d in field_data:
        field_obj = {
            "project_id" : project_id,
            "obj_id" : obj_id,
            "segement_id" : segment_id,
            "sap_structure" : d[3],
            "fields" : d[0],
            "description" : d[1],
            "isMandatory" : d[2],
            "isKey" : d[4]
        }
        field_check = fields.objects.filter(project_id=project_id,obj_id=obj_id,segement_id=segment_id)
        y=0
        for f in field_check:
            if f.fields == d[0]:
                y=1
                break
        if y==0:
            field_instance = FieldSerializer(data=field_obj)
            if field_instance.is_valid():
                field_id_get = field_instance.save()
                field_id = field_id_get.field_id
            else:
                return Response("Error at Field Creation")
        else:
            field_obj = {
                            "field_id" : f.field_id,
                            "project_id" : project_id,
                            "obj_id" : obj_id,
                            "segement_id" : segment_id,
                            "sap_structure" : d[3],
                            "fields" : d[0],
                            "description" : d[1],
                            "isMandatory" : d[2],
                            "isKey" : d[4]
                        }
            field = fields.objects.get(field_id=f.field_id)
            # print("Fields : ", field_obj)
            data = FieldSerializer(instance=field, data=field_obj)
            if data.is_valid():
                # print("Valid data")
                data.save()
            else:
                return Response("Error at Field Creation")
    field_inst = Rule.objects.filter(project_id=project_id,object_id=obj_id,segment_id=segment_id)
    if field_inst:
        latest_version = Rule.objects.filter(
            project_id=project_id,  # Assuming all items have the same IDs
            object_id=obj_id,
            segment_id=segment_id
        ).order_by('-version_id').first()
        field_inst = fields.objects.filter(project_id=project_id,obj_id=obj_id,segement_id=segment_id)
        for fi in field_inst:
            fields_tab = Rule.objects.filter(project_id=project_id,object_id=obj_id,segment_id=segment_id,version_id=latest_version.version_id,field_id=fi.field_id).first()
            if fields_tab:
                rule = {
                    "project_id" : project_id,
                    "object_id" : obj_id,
                    "segment_id" : segment_id,
                    "field_id" : fi.field_id,
                    "version_id" : latest_version.version_id+1,
                    "target_sap_table" : fi.sap_structure,
                    "target_sap_field" : fi.fields,
                    "source_table" : fields_tab.source_table,
                    "source_field_name" : fields_tab.source_field_name,
                    "data_mapping_rules": fields_tab.data_mapping_rules,
                    "text_description" : fi.description
                }
                sezr = RuleSerializer(data=rule)
                if sezr.is_valid():
                    sezr.save()
            else:
                rule = {
                    "project_id" : project_id,
                    "object_id" : obj_id,
                    "segment_id" : segment_id,
                    "field_id" : fi.field_id,
                    "version_id" : latest_version.version_id+1,
                    "target_sap_table" : fi.sap_structure,
                    "target_sap_field" : fi.fields,
                    "text_description" : fi.description
                }
                sezr = RuleSerializer(data=rule)
                if sezr.is_valid():
                    sezr.save()  
    return target_tables    
 
 
 
 
def sheet_delete(df,sheet_data,obj_id):
 
 
    # deleteSqlLiteTable()
   
    project_id = sheet_data['project_id']
    obj_name = sheet_data['obj_name']
    template_name  = sheet_data['template_name']
 
    x=0
    is_seg=0
    columns = []
    # segment = "Additional Descriptions"
    sheet_names = []
    for ind,i in df.iterrows():
        # print(i['Sheet Name'] , " : " , i['Sheet Name']!="" and i['Sheet Name'] == segment)
        if i['Sheet Name']=="":
            pass
        else:
            sheet_names.append(i['Sheet Name'])
 
    print("Sheets : ",sheet_names)
 
    segment_instance = segments.objects.filter(project_id=project_id,obj_id=obj_id)
    for s in segment_instance:
        if s.segement_name in sheet_names:
            pass
        else:
            seg_delete = SegementSerializer(s)
            s.delete()




@api_view(['GET'])
def RuleVersions(request,pid,oid,sid):
    latest_version = Rule.objects.filter(
                project_id=pid,  # Assuming all items have the same IDs
                object_id=oid,
                segment_id=sid
            ).order_by('-version_id').first()
 
    # print(latest_version.version_id)
    l=[]
    if latest_version:
        for i in range(latest_version.version_id):
            l.append({'ind':i+1})
        print("Hello : ",l)
        return Response(l)
    else:
        return Response(l)
 
 
@api_view(['GET'])
def VerisonData(request,pid,oid,sid,vid):
    print(vid)
    versiondata = Rule.objects.filter(
                project_id=pid,  # Assuming all items have the same IDs
                object_id=oid,
                segment_id=sid,
        version_id = vid    )
 
    versiondata = RuleSerializer(versiondata,many=True)
    return Response(versiondata.data)
 
   
@api_view(['POST'])
def SaveRuleCreate(request):
    data = request.data
    # print("Hello called SaveRuleCreate")
    for item in data:
        # 1. Check if a record with the same criteria exists
        # print("Hello : ",item)
        existing_record = SaveRule.objects.filter(
            project_id=item['project_id'],
            object_id=item['object_id'],
            segment_id=item['segment_id'],
            field_id = item['field_id']
        ).first()
        print(existing_record)
    
        item['check_box'] = False
 
 
        now = timezone.now()
        formatted_datetime = now.strftime("%Y-%m-%d %H:%M:%S")  # yyyy-mm-dd hh:mm:ss
        item['last_updated_on'] = formatted_datetime
 
        if existing_record:
            # 2. Update existing record
            serializer = SaveRuleSerializer(existing_record, data=item, partial=True)  # partial=True for partial updates
            # print("Hello Existing : ",serializer)
            if serializer.is_valid():
                serializer.save()
               
               
            else:
                print(serializer.error_messages)
                return Response(status = status.HTTP_404_NOT_FOUND)
 
        else:
            # 3. Create a new record
            serializer = SaveRuleSerializer(data=item)
            if serializer.is_valid():
                serializer.save()
               
            else:
                print(serializer.error_messages)
                return Response(status = status.HTTP_404_NOT_FOUND)
    return Response(status = status.HTTP_200_OK)
 
 
@api_view(['GET'])
def GetSaveRule(request,pid,oid,sid):
 
   
    serializer = SaveRule.objects.filter(
            project_id = pid,
            object_id = oid,
            segment_id = sid
        )
    if serializer:
        rule = SaveRuleSerializer(serializer,many=True)
        return Response(rule.data,status=status.HTTP_200_OK)
    else:
        return Response(status=status.HTTP_404_NOT_FOUND)
 
@api_view(['POST'])
def VersionRuleCreate(request):
 
    print("Hello called Version create")
    data=request.data
    latest_version = Rule.objects.filter(
                project_id=data[0]['project_id'],  # Assuming all items have the same IDs
                object_id=data[0]['object_id'],
                segment_id=data[0]['segment_id']
            ).order_by('-version_id').first()
    next_version_id = 1  # Default if no previous versions
    if latest_version:
        next_version_id = latest_version.version_id + 1
    for item in data:
            item['version_id'] = next_version_id
            item['check_box'] = False
    serializer = RuleSerializer(data=data, many=True)
    if serializer.is_valid():
        serializer.save()
        return Response(serializer.data, status=status.HTTP_201_CREATED)
    else:
        return Response(status=status.HTTP_404_NOT_FOUND)
 

@api_view(['GET'])
def getTableData(request, pid, oid, sid):
    print("cccccccccccccccccccccccccccccccccccooooocc")
    try:
        segment = segments.objects.filter(project_id=pid, obj_id=oid, segment_id=sid).first()  # Use .first() to get a single object
        if not segment:  # Handle the case where no segment is found.
            return Response({"error": "Segment not found"}, status=404)
 
        table_name = segment.table_name # Access table_name directly from the segment object
        print(table_name)
        print("hiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
        with connections["default"].cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table_name}")
            data = cursor.fetchall()
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
            column_names = [col[0] for col in cursor.description]
 
            # 1. Convert to list of dictionaries (JSON-like)    
            results = []
            for row in data:
                row_dict = dict(zip(column_names, row))  
                results.append(row_dict)
            return Response(results)  
 
    except Exception as e:
        print(f"Error fetching data: {e}")
        return Response({"error": str(e)}, status=500)
 

 

@api_view(['POST'])
def execute_queries(request,pid,oid,sid):
    prompt = request.data['prompt']
    print(prompt," ",pid," ",oid," ",sid)

    LLM_migration.project_id = pid
    LLM_migration.object_id = oid
    LLM_migration.segment_id = sid
    LLM_migration.user_prompt_from_backend = prompt

    LLM_migration.main()
    return Response("Hello")


def delete_table_data(table_name):

    try:
        with connection.cursor() as cursor:
                
            sql = f"DELETE FROM {table_name};"

            cursor.execute(sql)
            print(f"Data deleted from table '{table_name}' successfully.")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")


def update_related_data_with_mapping_and_composite_pks(table1_name, table2_name, field_mapping, condition1, pk_columns1, pk_columns2):
    try:
        with connections["default"].cursor() as cursor:
            # 1. Get data from table1 along with composite primary key
            select_columns = ", ".join(pk_columns1 + list(field_mapping.keys()))  # Combine PKs and fields
            select_sql = f"SELECT {select_columns} FROM {table1_name}"
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            print(len(rows))
            if not rows:
                print(f"No matching data found in {table1_name} for condition: {condition1}")
                return

            # 2. Update table2 for each row fetched from table1
            for row in rows:
                print(row)
                pk_values = row[:len(pk_columns1)]  # Extract primary key values (tuple)
                values_to_update = row[len(pk_columns1):]  # Extract values to update
                print("primary keys",pk_values)
                print("values to update",values_to_update)
                set_clause_parts = []
                for i, table1_field in enumerate(field_mapping.keys()):
                    table2_field = field_mapping[table1_field]
                    set_clause_parts.append(f"{table2_field} = %s")
                set_clause = ", ".join(set_clause_parts)
                pk_condition_parts = []
                for i, pk_col in enumerate(pk_columns2):
                    pk_condition_parts.append(f"{pk_col} = %s")
                condition2 = " AND ".join(pk_condition_parts)
                update_sql = f"UPDATE {table2_name} SET {set_clause} WHERE {condition2}"
                print(update_sql)
                cursor.execute(update_sql, list(values_to_update) + list(pk_values))  
            rows_updated = cursor.rowcount  
            print(rows_updated)
            connections["default"].commit()
            print(f"Successfully updated data in {table2_name} based on {table1_name}")

    except Exception as e:
        connections["default"].rollback()
        print(f"Error updating related data: {e}")


def remove_duplicate_rows_group_by_all(table_name):
    try:
        with connections["default"].cursor() as cursor:
            # 1. Get all column names
            cursor.execute(f"PRAGMA table_info({table_name})") #sqlite command
            columns = [row[1] for row in cursor.fetchall()] #get the column names

            if not columns:
                print(f"Table '{table_name}' not found or has no columns.")
                return

            # 2. Construct the SQL query
            columns_str = ", ".join(columns)
            query = f"""
                DELETE FROM {table_name}
                WHERE ROWID NOT IN (
                    SELECT MIN(ROWID)
                    FROM {table_name}
                    GROUP BY {columns_str}
                );
            """

            # 3. Execute the query
            cursor.execute(query)
            rows_deleted = cursor.rowcount
            connections["default"].commit()
            print(f"Removed {rows_deleted} duplicate rows from '{table_name}'.")

    except Exception as e:
        connections["default"].rollback()
        print(f"Error removing duplicates: {e}")



def copy_data_between_tables_with_field_mapping(table1_name, table2_name, field_mapping):
    try:
        with connections["default"].cursor() as cursor:
            # 1. Construct the INSERT and SELECT queries dynamically
            select_clause = ", ".join(field_mapping.keys())  # Select from table1
            insert_columns = ", ".join(field_mapping.values())  # Insert into table2

            insert_sql = f"INSERT INTO {table2_name} ({insert_columns}) SELECT {select_clause} FROM {table1_name}"

            # 2. Execute the query
            cursor.execute(insert_sql)

            rows_copied = cursor.rowcount
            connections["default"].commit()
            print(f"Successfully copied {rows_copied} rows from {table1_name} to {table2_name}")

    except Exception as e:
        connections["default"].rollback()
        print(f"Error copying data: {e}")



def create_and_insert_data(table_name, data):
    if not data:
        print("No data provided. Table creation and insertion skipped.")
        return
    try:
        with connections["default"].cursor() as cursor:
            # 1. Create Table (Simplified - Same structure assumed)
            first_row = data[0]
            drop_table_dynamically(table_name=table_name)
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("

            columns = []
            for key, value in first_row.items():  # Iterate through the first row only
                column_name = key
                column_type = "TEXT"  # Default type (you can refine this based on value types)
                columns.append(f'"{column_name}" {column_type}')

            create_table_sql += ", ".join(columns) + ")"
            cursor.execute(create_table_sql)


            # 2. Insert Data (Same as before)
            insert_sql = f"INSERT INTO {table_name} (" + ", ".join(f'"{key}"' for key in first_row) + ") VALUES (" + ", ".join(["%s"] * len(first_row)) + ")"

            for row in data:
                values = list(row.values())
                cursor.execute(insert_sql, values)

            connections["default"].commit()
            print(f"Table '{table_name}' created and data inserted successfully.")

    except Exception as e:
        connections["default"].rollback()
        print(f"Error creating/inserting data: {e}")



def func(table_name):
  table_name = table_name.upper()
  class RFC_ERROR_INFO(Structure):
      _fields_ = [("code", c_long),
                  ("group", c_long),
                  ("key", c_wchar * 128),
                  ("message", c_wchar * 512),
                  ("abapMsgClass", c_wchar * 21),
                  ("abapMsgType", c_wchar * 2),
                  ("abapMsgNumber", c_wchar * 4),
                  ("abapMsgV1", c_wchar * 51),
                  ("abapMsgV2", c_wchar * 51),
                  ("abapMsgV3", c_wchar * 51),
                  ("abapMsgV4", c_wchar * 51)]

  class RFC_CONNECTION_PARAMETER(Structure):
      _fields_ = [("name", c_wchar_p),
                  ("value", c_wchar_p)]


  #-Constants-------------------------------------------------------------

  #-RFC_RC - RFC return codes---------------------------------------------
  RFC_OK = 0
  RFC_COMMUNICATION_FAILURE = 1
  RFC_LOGON_FAILURE = 2
  RFC_ABAP_RUNTIME_FAILURE = 3
  RFC_ABAP_MESSAGE = 4
  RFC_ABAP_EXCEPTION = 5
  RFC_CLOSED = 6
  RFC_CANCELED = 7
  RFC_TIMEOUT = 8
  RFC_MEMORY_INSUFFICIENT = 9
  RFC_VERSION_MISMATCH = 10
  RFC_INVALID_PROTOCOL = 11
  RFC_SERIALIZATION_FAILURE = 12
  RFC_INVALID_HANDLE = 13
  RFC_RETRY = 14
  RFC_EXTERNAL_FAILURE = 15
  RFC_EXECUTED = 16
  RFC_NOT_FOUND = 17
  RFC_NOT_SUPPORTED = 18
  RFC_ILLEGAL_STATE = 19
  RFC_INVALID_PARAMETER = 20
  RFC_CODEPAGE_CONVERSION_FAILURE = 21
  RFC_CONVERSION_FAILURE = 22
  RFC_BUFFER_TOO_SMALL = 23
  RFC_TABLE_MOVE_BOF = 24
  RFC_TABLE_MOVE_EOF = 25
  RFC_START_SAPGUI_FAILURE = 26
  RFC_ABAP_CLASS_EXCEPTION = 27
  RFC_UNKNOWN_ERROR = 28
  RFC_AUTHORIZATION_FAILURE = 29

  #-RFCTYPE - RFC data types----------------------------------------------
  RFCTYPE_CHAR = 0
  RFCTYPE_DATE = 1
  RFCTYPE_BCD = 2
  RFCTYPE_TIME = 3
  RFCTYPE_BYTE = 4
  RFCTYPE_TABLE = 5
  RFCTYPE_NUM = 6
  RFCTYPE_FLOAT = 7
  RFCTYPE_INT = 8
  RFCTYPE_INT2 = 9
  RFCTYPE_INT1 = 10
  RFCTYPE_NULL = 14
  RFCTYPE_ABAPOBJECT = 16
  RFCTYPE_STRUCTURE = 17
  RFCTYPE_DECF16 = 23
  RFCTYPE_DECF34 = 24
  RFCTYPE_XMLDATA = 28
  RFCTYPE_STRING = 29
  RFCTYPE_XSTRING = 30
  RFCTYPE_BOX = 31
  RFCTYPE_GENERIC_BOX = 32

  #-RFC_UNIT_STATE - Processing status of a background unit---------------
  RFC_UNIT_NOT_FOUND = 0 
  RFC_UNIT_IN_PROCESS = 1 
  RFC_UNIT_COMMITTED = 2 
  RFC_UNIT_ROLLED_BACK = 3 
  RFC_UNIT_CONFIRMED = 4 

  #-RFC_CALL_TYPE - Type of an incoming function call---------------------
  RFC_SYNCHRONOUS = 0 
  RFC_TRANSACTIONAL = 1 
  RFC_QUEUED = 2 
  RFC_BACKGROUND_UNIT = 3 

  #-RFC_DIRECTION - Direction of a function module parameter--------------
  RFC_IMPORT = 1 
  RFC_EXPORT = 2 
  RFC_CHANGING = RFC_IMPORT + RFC_EXPORT 
  RFC_TABLES = 4 + RFC_CHANGING 

  #-RFC_CLASS_ATTRIBUTE_TYPE - Type of an ABAP object attribute-----------
  RFC_CLASS_ATTRIBUTE_INSTANCE = 0 
  RFC_CLASS_ATTRIBUTE_CLASS = 1 
  RFC_CLASS_ATTRIBUTE_CONSTANT = 2 

  #-RFC_METADATA_OBJ_TYPE - Ingroup repository----------------------------
  RFC_METADATA_FUNCTION = 0 
  RFC_METADATA_TYPE = 1 
  RFC_METADATA_CLASS = 2 


  #-Variables-------------------------------------------------------------
  ErrInf = RFC_ERROR_INFO; RfcErrInf = ErrInf()
  ConnParams = RFC_CONNECTION_PARAMETER * 5; RfcConnParams = ConnParams()
  SConParams = RFC_CONNECTION_PARAMETER * 3; RfcSConParams = SConParams()


  #-Library---------------------------------------------------------------
  # if str(platform.architecture()[0]) == "32bit":
  #   os.environ['PATH'] += ";C:\\SAPRFCSDK\\32bit"
  #   SAPNWRFC = "C:\\SAPRFCSDK\\32bit\\sapnwrfc.dll"
  # elif str(platform.architecture()[0]) == "64bit":
  #   os.environ['PATH'] += ";C:\\SAPRFCSDK\\64bit"
  #   SAPNWRFC = "C:\\SAPRFCSDK\\64bit\\sapnwrfc.dll"

  SAPNWRFC = "sapnwrfc.dll"
  SAP = windll.LoadLibrary(SAPNWRFC)

  #-Prototypes------------------------------------------------------------
  SAP.RfcAppendNewRow.argtypes = [c_void_p, POINTER(ErrInf)]
  SAP.RfcAppendNewRow.restype = c_void_p

  SAP.RfcCreateTable.argtypes = [c_void_p, POINTER(ErrInf)]
  SAP.RfcCreateTable.restype = c_void_p

  SAP.RfcCloseConnection.argtypes = [c_void_p, POINTER(ErrInf)]
  SAP.RfcCloseConnection.restype = c_ulong

  SAP.RfcCreateFunction.argtypes = [c_void_p, POINTER(ErrInf)]
  SAP.RfcCreateFunction.restype = c_void_p

  SAP.RfcCreateFunctionDesc.argtypes = [c_wchar_p, POINTER(ErrInf)]
  SAP.RfcCreateFunctionDesc.restype = c_void_p

  SAP.RfcDestroyFunction.argtypes = [c_void_p, POINTER(ErrInf)]
  SAP.RfcDestroyFunction.restype = c_ulong

  SAP.RfcDestroyFunctionDesc.argtypes = [c_void_p, POINTER(ErrInf)]
  SAP.RfcDestroyFunctionDesc.restype = c_ulong

  SAP.RfcGetChars.argtypes = [c_void_p, c_wchar_p, c_void_p, c_ulong, \
    POINTER(ErrInf)]
  SAP.RfcGetChars.restype = c_ulong

  SAP.RfcGetCurrentRow.argtypes = [c_void_p, POINTER(ErrInf)]
  SAP.RfcGetCurrentRow.restype = c_void_p

  SAP.RfcGetFunctionDesc.argtypes = [c_void_p, c_wchar_p, POINTER(ErrInf)]
  SAP.RfcGetFunctionDesc.restype = c_void_p

  SAP.RfcGetRowCount.argtypes = [c_void_p, POINTER(c_ulong), \
    POINTER(ErrInf)]
  SAP.RfcGetRowCount.restype = c_ulong

  SAP.RfcGetStructure.argtypes = [c_void_p, c_wchar_p, \
    POINTER(c_void_p), POINTER(ErrInf)]
  SAP.RfcGetStructure.restype = c_ulong

  SAP.RfcGetTable.argtypes = [c_void_p, c_wchar_p, POINTER(c_void_p), \
    POINTER(ErrInf)]
  SAP.RfcGetTable.restype = c_ulong

  SAP.RfcGetVersion.argtypes = [POINTER(c_ulong), POINTER(c_ulong), \
    POINTER(c_ulong)]
  SAP.RfcGetVersion.restype = c_wchar_p

  SAP.RfcInstallServerFunction.argtypes = [c_wchar_p, c_void_p, \
    c_void_p, POINTER(ErrInf)]
  SAP.RfcInstallServerFunction.restype = c_ulong

  SAP.RfcInvoke.argtypes = [c_void_p, c_void_p, POINTER(ErrInf)]
  SAP.RfcInvoke.restype = c_ulong

  SAP.RfcListenAndDispatch.argtypes = [c_void_p, c_ulong, POINTER(ErrInf)]
  SAP.RfcListenAndDispatch.restype = c_ulong

  SAP.RfcMoveToFirstRow.argtypes = [c_void_p, POINTER(ErrInf)]
  SAP.RfcMoveToFirstRow.restype = c_ulong

  SAP.RfcMoveToNextRow.argtypes = [c_void_p, POINTER(ErrInf)]
  SAP.RfcMoveToNextRow.restype = c_ulong

  SAP.RfcOpenConnection.argtypes = [POINTER(ConnParams), c_ulong, \
    POINTER(ErrInf)]
  SAP.RfcOpenConnection.restype = c_void_p

  SAP.RfcPing.argtypes = [c_void_p, POINTER(ErrInf)]
  SAP.RfcPing.restype = c_ulong

  SAP.RfcRegisterServer.argtypes = [POINTER(SConParams), c_ulong, \
    POINTER(ErrInf)]
  SAP.RfcRegisterServer.restype = c_void_p

  SAP.RfcSetChars.argtypes = [c_void_p, c_wchar_p, c_wchar_p, c_ulong, \
    POINTER(ErrInf)]
  SAP.RfcSetChars.restype = c_ulong
  def join_json_objects_multiple_keys(obj1, obj2, primary_keys):
      result = []

      # Create a dictionary to efficiently look up items in obj2 by combined primary keys
      obj2_lookup = {}
      for item2 in obj2:
          key = tuple(item2[key] for key in primary_keys)  # Create a tuple key
          obj2_lookup[key] = item2

      for item1 in obj1:
          key = tuple(item1[key] for key in primary_keys)
          item2 = obj2_lookup.get(key)  # Efficient lookup

          if item2:
              merged_object = {**item1, **item2}
              result.append(merged_object)
          else:
              result.append(item1)  # Keep item1 if no match
              print(f"No match found for {key}")

      return result


  #-Main------------------------------------------------------------------

  RfcConnParams[0].name = "ASHOST"; RfcConnParams[0].value = "34.194.191.113"
  RfcConnParams[1].name = "SYSNR" ; RfcConnParams[1].value = "01"
  RfcConnParams[2].name = "CLIENT"; RfcConnParams[2].value = "100"
  RfcConnParams[3].name = "USER"  ; RfcConnParams[3].value = "RAJKUMARS"
  RfcConnParams[4].name = "PASSWD"; RfcConnParams[4].value = "JaiHanuman10"

  TableName = table_name
  keyFields = []
  cnt = 0

  hRFC = SAP.RfcOpenConnection(RfcConnParams, 5, RfcErrInf)
  if hRFC != None:

    charBuffer = create_unicode_buffer(1048576 + 1)

    hFuncDesc = SAP.RfcGetFunctionDesc(hRFC, "CACS_GET_TABLE_FIELD450", RfcErrInf)
    if hFuncDesc != 0:
      hFunc = SAP.RfcCreateFunction(hFuncDesc, RfcErrInf)
      if hFunc != 0:

        rc = SAP.RfcSetChars(hFunc, "I_TABNAME", TableName, \
          len(TableName), RfcErrInf)
        print(SAP.RfcInvoke(hRFC, hFunc, RfcErrInf) == RFC_OK)
        if SAP.RfcInvoke(hRFC, hFunc, RfcErrInf) == RFC_OK:

          hTable = c_void_p(0)
          if SAP.RfcGetTable(hFunc, "T_KEYFIELD", hTable, RfcErrInf) == RFC_OK:
            RowCount = c_ulong(0)
            rc = SAP.RfcGetRowCount(hTable, RowCount, RfcErrInf)
            print(RowCount, 1)
            rc = SAP.RfcMoveToFirstRow(hTable, RfcErrInf)
            for i in range(0, RowCount.value):
              hRow = SAP.RfcGetCurrentRow(hTable, RfcErrInf)
              rc = SAP.RfcGetChars(hRow, "FIELDNAME", charBuffer, 512, RfcErrInf)
              # print(str(charBuffer.value), end="    ")
              fieldName = str(charBuffer.value)
              # rc = SAP.RfcGetChars(hRow, "LENGTH", charBuffer, 512, RfcErrInf)
              # val = int(charBuffer.value)
              # if (sum + val < 512):
              #   sum += val
              #   l1.append(fieldName.strip())
              #   # print(sum)
              # else:
              keyFields.append(fieldName.strip())
                # l1 = [fieldName.strip()]
                # sum = val
              if i < RowCount.value:
                rc = SAP.RfcMoveToNextRow(hTable, RfcErrInf)

        rc = SAP.RfcDestroyFunction(hFunc, RfcErrInf)

    # rc = SAP.RfcCloseConnection(hRFC, RfcErrInf)

    print(*keyFields)

    keyFieldsCnt = len(keyFields)
    print(keyFieldsCnt)
  else:
    print(RfcErrInf.key)
    print(RfcErrInf.message)


  ind, keyDict = 0, {}

  # hRFC = SAP.RfcOpenConnection(RfcConnParams, 5, RfcErrInf)
  if hRFC != None:

    charBuffer = create_unicode_buffer(1048576 + 1)

    hFuncDesc = SAP.RfcGetFunctionDesc(hRFC, "Z450RFC_READ_TABLE", RfcErrInf)
    if hFuncDesc != 0:
      hFunc = SAP.RfcCreateFunction(hFuncDesc, RfcErrInf)
      if hFunc != 0:

        rc = SAP.RfcSetChars(hFunc, "QUERY_TABLE", TableName, \
          len(TableName), RfcErrInf)
        rc = SAP.RfcSetChars(hFunc, "DELIMITER", "~", 1, RfcErrInf)
        if SAP.RfcInvoke(hRFC, hFunc, RfcErrInf) == RFC_OK:

          hTable = c_void_p(0)
          if SAP.RfcGetTable(hFunc, "FIELDS", hTable, RfcErrInf) == RFC_OK:
            
            
            sum, l, l1 = 0, [], keyFields.copy()
            keyFieldsLen = 0
            RowCount = c_ulong(0)
            rc = SAP.RfcGetRowCount(hTable, RowCount, RfcErrInf)
            print(RowCount)
            rc = SAP.RfcMoveToFirstRow(hTable, RfcErrInf)
            for i in range(0, RowCount.value):
              hRow = SAP.RfcGetCurrentRow(hTable, RfcErrInf)
              rc = SAP.RfcGetChars(hRow, "FIELDNAME", charBuffer, 512, RfcErrInf)
              # print(str(charBuffer.value), end="    ")
              fieldName = str(charBuffer.value)
              rc = SAP.RfcGetChars(hRow, "LENGTH", charBuffer, 512, RfcErrInf)
              val = int(charBuffer.value)
              cnt += 1
              # print(fieldName.strip(), cnt)
              if (i < keyFieldsCnt):
                print(i)
                i += 1
                keyFieldsLen += val
              else:
                if (sum + val + keyFieldsLen < 400):
                  sum += val
                  l1.append(fieldName.strip())
                  # print(sum)
                else:
                  l.append(l1)
                  l1 = keyFields.copy()
                  l1.append(fieldName.strip())
                  # print(sum + keyFieldsLen)
                  sum = val
                  
              if i < RowCount.value:
                rc = SAP.RfcMoveToNextRow(hTable, RfcErrInf)
            l.append(l1)
        rc = SAP.RfcDestroyFunction(hFunc, RfcErrInf)

    # rc = SAP.RfcCloseConnection(hRFC, RfcErrInf)

    # print(l)
  else:
    print(RfcErrInf.key)
    print(RfcErrInf.message)

  # for i in l:
  #   print(i[:2])

  length = 0
  for ii in l:
    for jj in ii:
      if (jj == 'MANDT' or jj == 'MATNR'): continue
      length += 1
  print(length)

  jsonTemp = jsonPrimary = []
  for splittedFields in l:
    # hRFC = SAP.RfcOpenConnection(RfcConnParams, 5, RfcErrInf)
    if hRFC != None:

      charBuffer = create_unicode_buffer(1048576 + 1)

      hFuncDesc = SAP.RfcGetFunctionDesc(hRFC, "Z450RFC_READ_TAB_DATA", RfcErrInf)
      if hFuncDesc != 0:
        hFunc = SAP.RfcCreateFunction(hFuncDesc, RfcErrInf)
        if hFunc != 0:

          rc = SAP.RfcSetChars(hFunc, "QUERY_TABLE", TableName, \
            len(TableName), RfcErrInf)
          rc = SAP.RfcSetChars(hFunc, "DELIMITER", "~", 1, RfcErrInf)

          #MATNR,MTART,ATTYP,SATNR,MATKL,MBRSH,MEINS,SPART,BISMT,DATAB,LIQDT,NORMT,GROES,LABOR,BRGEW,NTGEW,GEWEI,LAENG,BREIT,HOEHE,MEABM,VOLUM,VOLEH,KZKFG,IPRKZ,RAUBE,TEMPB,BEHVO,STOFF,ETIAR,ETIFO,WESCH,XGCHP,MHDHB,MHDRZ,SLED_BBD

          field = ','.join(splittedFields)
          # print(field)
          rc = SAP.RfcSetChars(hFunc, "FIELDNAME", field, len(field), RfcErrInf)

          # print(SAP.RfcInvoke(hRFC, hFunc, RfcErrInf) == RFC_OK)
          if SAP.RfcInvoke(hRFC, hFunc, RfcErrInf) == RFC_OK:

            hTable = c_void_p(0)
            if SAP.RfcGetTable(hFunc, "DATA", hTable, RfcErrInf) == RFC_OK:

              RowCount = c_ulong(0)
              rc = SAP.RfcGetRowCount(hTable, RowCount, RfcErrInf)
              rc = SAP.RfcMoveToFirstRow(hTable, RfcErrInf)
              for i in range(0, RowCount.value):
                hRow = SAP.RfcGetCurrentRow(hTable, RfcErrInf)
                rc = SAP.RfcGetChars(hRow, "WA", charBuffer, 1024, RfcErrInf)
                data_row=charBuffer.value
                # data_dict = {field: value for field, value in zip(splittedFields, data_row)}
                # print(data_dict)

                data_row = charBuffer.value.split("~")
  
                                  # Create dictionary using only requested fields
                              # data_dict = {field: value for field, value in zip(field, data_row)}
                              # # print(charBuffer.value)
                              # res.append(data_dict)
                fields = field.split(",")
                data_dict = {f: v.strip() for f, v in zip(fields, data_row)}
                jsonTemp.append(data_dict)

                if i < RowCount.value:
                  rc = SAP.RfcMoveToNextRow(hTable, RfcErrInf)

          rc = SAP.RfcDestroyFunction(hFunc, RfcErrInf)
      # print(jsonTemp)
      if (jsonPrimary == []): 
        jsonPrimary = jsonTemp
      else:
        jsonPrimary = join_json_objects_multiple_keys(jsonPrimary, jsonTemp, keyFields)
      jsonTemp = []
      # rc = SAP.RfcCloseConnection(hRFC, RfcErrInf)

    else:
      print(RfcErrInf.key)
      print(RfcErrInf.message)


  del SAP

  # for d in jsonPrimary:
  #   for j in d:
  #     d[j] = d[j].strip()

  return jsonPrimary


def table_exists(table_name):
    try:
        with connections["default"].cursor() as cursor:
            # SQLite specific query to check for table existence
            cursor.execute(f"SELECT * FROM {table_name}")
            return True

    except Exception as e:
        print(f"Error checking table existence: {e}")
        return False  


@api_view(['GET'])
def getChat(request,pid,oid,sid):
    chats = Chat.objects.filter(project_id=pid,object_id=oid,segment_id=sid).all()
    print(chats)
    chats = ChatSerializer(data=chats,many=True)
    chats.is_valid()
    return Response(chats.data)
   
 
@api_view(['POST'])
def CreateChat(request):
    data=request.data
    serializer_data = ChatSerializer(data=data)
    if(serializer_data.is_valid()):
        serializer_data.save()
        return Response("success")
    return Response("fail")


def update_column_with_constant(table_name, column_name, constant_value):
    try:
        with connection.cursor() as cursor:
            # Use parameterized query to prevent SQL injection
            cursor.execute(f"UPDATE {table_name} SET {column_name} = %s", [constant_value])
            print(f"Successfully updated {cursor.rowcount} rows in {table_name}.{column_name}")
 
    except Exception as e:
        print(f"Error updating column: {e}")


@api_view(['GET'])
def applyOneToOne(request,pid,oid,sid):
    print("gggjgg")
    try:
        print("came")
        tar_table=""
        latest_version = Rule.objects.filter(
                    project_id=pid,  
                    object_id=oid,
                    segment_id=sid
                ).order_by('-version_id').first()
        rules = Rule.objects.filter(project_id=pid,  
                    object_id=oid,
                    segment_id=sid,version_id=latest_version.version_id).all()
        field_mapping={}
        segmentForTable = segments.objects.filter(segment_id=sid,project_id=pid,obj_id=oid).first()
        target_table_name = segmentForTable.table_name
        print(latest_version.version_id)
        if(latest_version.version_id==1):
            for rule in rules:
                curr_field = fields.objects.filter(field_id=rule.field_id).first()
                isMandt=curr_field.isKey
                print("came1")
                if(rule.source_table!="" and rule.source_field_name!="" and isMandt != "False"):    
                    print(rule.source_table,rule.source_field_name,target_table_name,rule.target_sap_field)
                    if(not(table_exists(rule.source_table.upper()))):
                        jsonPrimary=func(rule.source_table.upper())      
                        create_and_insert_data(rule.source_table.upper(),jsonPrimary)
                    print("came2")
                    src_table=rule.source_table.upper()
                    tar_table=target_table_name
                    field_mapping[rule.source_field_name] = rule.target_sap_field
            # print(src_table,tar_table,field_mapping)
            if(src_table!="" and tar_table!=""):
                copy_data_between_tables_with_field_mapping(src_table,tar_table,field_mapping)
                remove_duplicate_rows_group_by_all(tar_table)
           
 
        field_mapping={}  
        print("hii")  
        rules = Rule.objects.filter(project_id=pid,  
                    object_id=oid,
                    segment_id=sid,version_id=1).all()
        for rule in rules :
            print("in cur rule")
            curr_field = fields.objects.filter(field_id=rule.field_id).first()
            isMandt=curr_field.isKey
            print("before")
            print(isMandt)
            if(isMandt != "False"):
                print("inside")
                print(rule.target_sap_field)
                field_mapping[rule.source_field_name] = rule.target_sap_field
        # field mapping contains all the mandetory fields from segment along with their techinal filed name
        rules = Rule.objects.filter(project_id=pid,  
                    object_id=oid,
                    segment_id=sid,version_id=latest_version.version_id).all()
        print(field_mapping)        
        for rule in rules:
            curr_field = fields.objects.filter(field_id=rule.field_id).first()
            isMandt=curr_field.isKey
            if(rule.source_table!="" and rule.source_field_name!="" and isMandt == "False"):
                if(not(table_exists(target_table_name.upper()))):
                    jsonPrimary=func(rule.source_table)  
                    print(jsonPrimary[0])
                    create_and_insert_data(rule.source_table.upper(),jsonPrimary)
                print(rule.source_table,rule.source_field_name,rule.target_sap_table,rule.target_sap_field)
                pkcol1=list(field_mapping.keys())
                pkcol2=list(field_mapping.values())
                print(pkcol1)
                print(pkcol2)
                print("tablename",segmentForTable.table_name)
                try:
                    update_related_data_with_mapping_and_composite_pks(rule.source_table,segmentForTable.table_name,
                                                        {rule.source_field_name:rule.target_sap_field} , " 1 = 1 ",pkcol1,pkcol2)
                except Exception as e:
                    return Response(str(e))
    except Exception as e:
        print(e)   
    latest_version = Rule.objects.filter(
                    project_id=pid,  
                    object_id=oid,
                    segment_id=sid
                ).order_by('-version_id').first()
    rules = Rule.objects.filter(project_id=pid,
                object_id=oid,
                segment_id=sid,version_id=latest_version.version_id).all()
    segmentForTable = segments.objects.filter(segment_id=sid,project_id=pid,obj_id=oid).first()
    target_table_name = segmentForTable.table_name
    for rule in rules:
        print(rule.data_mapping_type,rule.data_mapping_rules)  
        if(rule.data_mapping_type=='Constant'):
            update_column_with_constant(target_table_name,rule.target_sap_field,rule.data_mapping_rules)
    return Response("Hii")

@api_view(['GET']) 
def getLatestVersion(request,pid,oid,sid):
    print("came to latestVersion")
    latest_version = Rule.objects.filter(
                project_id=pid, 
                object_id=oid,
                segment_id=sid
            ).order_by('-version_id').first()
    if(not(latest_version)):
        return Response([])
    versiondata = Rule.objects.filter(
                project_id=pid,  
                object_id=oid,
                segment_id=sid,
        version_id = latest_version.version_id).all()
    versiondata = RuleSerializer(versiondata,many=True)
    temp=[]
    for i in versiondata.data:
        if i['data_mapping_type'] != "":
            temp.append(i)
    print("rrrrrrrrttttttt",versiondata)        
    return Response(temp)



@api_view(['POST'])
def saveSuccessFactors(request):
    project_id = request.data['project_id']
    template_name = request.data['template_name']
    template_name = TableName_Modification(template_name)
    # project_id = 12
    # template_name = "PersonalInfo"
    file = request.FILES['file']
   
    obj_name = f"{template_name}_object"
 
    check = objects.objects.filter(project_id=project_id,obj_name=obj_name)
    if check:
        return Response(status=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE)
 
 
 
    data = pd.read_csv(file)
    column = list(data.columns)
    col = []
    columns = []
    for c in column:
        col.append(c)
        col.append("TEXT")
        columns.append(col)
        col = []
    # print(*columns, sep = ', ')
    data = pd.DataFrame(data)
 
    df = pd.DataFrame(data)
    print(df)
 
    seg_name = f"{template_name}_segment"
 
    obj_data = {
        "obj_name" : obj_name,
        "project_id" : project_id,
        "template_name" : template_name
    }
 
    # print(obj_data)
 
    # print("Heloooooooooooooo")
    obj = ObjectSerializer(data=obj_data)
 
   
    if objects.objects.filter(project_id=obj_data['project_id'],obj_name = obj_data['obj_name']):
        return Response(status=status.HTTP_406_NOT_ACCEPTABLE)
 
    if obj.is_valid():
        obj_instance=obj.save()
        objid = obj_instance.obj_id
 
        # seg =TableName_Modification(seg_name)
        # objName =TableName_Modification(obj_name)
        tab ="t"+"_"+str(project_id)+"_"+str(obj_name)+"_"+str(seg_name)
        seg = seg_name
        seg_obj = {
            "project_id" : project_id,
            "obj_id" : objid,
            "segement_name":seg,
            "table_name" : tab
        }
        seg_instance = SegementSerializer(data=seg_obj)
        if seg_instance.is_valid():
            seg_id_get = seg_instance.save()
            segment_id = seg_id_get.segment_id
 
        else:
            return Response("Error at first segement creation")
       
 
        create_table(tab,columns)
 
        for d in columns:
            field_obj = {
                "project_id" : project_id,
                "obj_id" : objid,
                "segement_id" : segment_id,
                "sap_structure" : "",
                "fields" : d[0],
                "description" : "",
                "isMandatory" : "True",
                "isKey" : ""
            }
            field_instance = FieldSerializer(data=field_obj)
            if field_instance.is_valid():
                field_id_get = field_instance.save()
                field_id = field_id_get.field_id
            else:
                return Response("Error at Field Creation")
 
        insert_data_from_dataframe(df,tab)
 
        return Response(obj.data)
    else:
        return Response(status=status.HTTP_409_CONFLICT) 




@api_view(['PUT'])
def reUploadSuccessFactors(request,oid):
    template_name = request.data['template_name']
    # project_id = 12
    # template_name = "Personal Info"
    template_name = TableName_Modification(template_name)
    file = request.FILES['file']
 
    obj_name = f"{template_name}_object"
 
    obj = objects.objects.filter(obj_id = oid)
 
 
    if obj:
        obj = objects.objects.get(obj_id = oid)
        serializer = ObjectSerializer(obj)
        project_id = serializer.data['project_id']
        if obj.obj_name != obj_name :
            print("Hii inside if")
            check = objects.objects.filter(project_id=project_id,obj_name=obj_name)
            if check:
                return Response(status=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE)
       
        seg_delete = segments.objects.filter(project_id=project_id,obj_id = oid)
        if seg_delete:
            seg_del = segments.objects.get(project_id=project_id,obj_id = oid)
            deleteSqlLiteTable(seg_del.table_name)      
            if seg_del:
                serializer = SegementSerializer(seg_del)
                seg_del.delete()
 
       
 
        obj_data = {
            "obj_name" : obj_name,
            "project_id" : project_id,
            "template_name" : template_name
        }
       
        data = ObjectSerializer(instance=obj, data=obj_data)
        if data.is_valid():
            obj_instance=data.save()
            objid = obj_instance.obj_id
           
 
            data = pd.read_csv(file)
            column = list(data.columns)
            col = []
            columns = []
            for c in column:
                col.append(c)
                col.append("TEXT")
                columns.append(col)
                col = []
            # print(*columns, sep = ', ')
            data = pd.DataFrame(data)
 
            df = pd.DataFrame(data)
            print(df)
 
            seg_name = f"{template_name}_segment"
 
 
 
            # seg =TableName_Modification(seg_name)
            # objName =TableName_Modification(obj_name)
            tab ="t"+"_"+str(project_id)+"_"+str(obj_name)+"_"+str(seg_name)
            seg = seg_name
            seg_obj = {
                "project_id" : project_id,
                "obj_id" : objid,
                "segement_name":seg,
                "table_name" : tab
            }
            seg_instance = SegementSerializer(data=seg_obj)
            if seg_instance.is_valid():
                seg_id_get = seg_instance.save()
                segment_id = seg_id_get.segment_id
 
            else:
                return Response("Error at first segement creation")
           
 
            create_table(tab,columns)
 
            for d in columns:
                field_obj = {
                    "project_id" : project_id,
                    "obj_id" : objid,
                    "segement_id" : segment_id,
                    "sap_structure" : "",
                    "fields" : d[0],
                    "description" : "",
                    "isMandatory" : "True",
                    "isKey" : ""
                }
                field_instance = FieldSerializer(data=field_obj)
                if field_instance.is_valid():
                    field_id_get = field_instance.save()
                    field_id = field_id_get.field_id
                else:
                    return Response("Error at Field Creation")
 
            insert_data_from_dataframe(df,tab)
 
            return Response("Done")
       
        else:
            return Response(status=status.HTTP_406_NOT_ACCEPTABLE)
 
    else:
        return Response(status=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE)



def join_json_objects_multiple_keys(obj1, obj2, primary_keys):
    result = []
 
    # Create a dictionary to efficiently look up items in obj2 by combined primary keys
    obj2_lookup = {}
    for item2 in obj2:
        key = tuple(item2[key] for key in primary_keys)  # Create a tuple key
        obj2_lookup[key] = item2
 
    for item1 in obj1:
        key = tuple(item1[key] for key in primary_keys)
        item2 = obj2_lookup.get(key)  # Efficient lookup
 
        if item2:
            merged_object = {**item1, **item2}
            result.append(merged_object)
        else:
            result.append(item1)  # Keep item1 if no match
            print(f"No match found for {key}")
 
    return result


@api_view(['GET'])
def getSapTableData(request):
    #-Begin-----------------------------------------------------------------
 
#-Packages--------------------------------------------------------------
 
 
    #-Constants-------------------------------------------------------------
 
    #-RFC_RC - RFC return codes---------------------------------------------
    class RFC_ERROR_INFO(Structure):
        _fields_ = [("code", c_long),
                    ("group", c_long),
                    ("key", c_wchar * 128),
                    ("message", c_wchar * 512),
                    ("abapMsgClass", c_wchar * 21),
                    ("abapMsgType", c_wchar * 2),
                    ("abapMsgNumber", c_wchar * 4),
                    ("abapMsgV1", c_wchar * 51),
                    ("abapMsgV2", c_wchar * 51),
                    ("abapMsgV3", c_wchar * 51),
                    ("abapMsgV4", c_wchar * 51)]
    class RFC_CONNECTION_PARAMETER(Structure):
        _fields_ = [("name", c_wchar_p),
                    ("value", c_wchar_p)]
    RFC_OK = 0
    RFC_COMMUNICATION_FAILURE = 1
    RFC_LOGON_FAILURE = 2
    RFC_ABAP_RUNTIME_FAILURE = 3
    RFC_ABAP_MESSAGE = 4
    RFC_ABAP_EXCEPTION = 5
    RFC_CLOSED = 6
    RFC_CANCELED = 7
    RFC_TIMEOUT = 8
    RFC_MEMORY_INSUFFICIENT = 9
    RFC_VERSION_MISMATCH = 10
    RFC_INVALID_PROTOCOL = 11
    RFC_SERIALIZATION_FAILURE = 12
    RFC_INVALID_HANDLE = 13
    RFC_RETRY = 14
    RFC_EXTERNAL_FAILURE = 15
    RFC_EXECUTED = 16
    RFC_NOT_FOUND = 17
    RFC_NOT_SUPPORTED = 18
    RFC_ILLEGAL_STATE = 19
    RFC_INVALID_PARAMETER = 20
    RFC_CODEPAGE_CONVERSION_FAILURE = 21
    RFC_CONVERSION_FAILURE = 22
    RFC_BUFFER_TOO_SMALL = 23
    RFC_TABLE_MOVE_BOF = 24
    RFC_TABLE_MOVE_EOF = 25
    RFC_START_SAPGUI_FAILURE = 26
    RFC_ABAP_CLASS_EXCEPTION = 27
    RFC_UNKNOWN_ERROR = 28
    RFC_AUTHORIZATION_FAILURE = 29
 
    #-RFCTYPE - RFC data types----------------------------------------------
    RFCTYPE_CHAR = 0
    RFCTYPE_DATE = 1
    RFCTYPE_BCD = 2
    RFCTYPE_TIME = 3
    RFCTYPE_BYTE = 4
    RFCTYPE_TABLE = 5
    RFCTYPE_NUM = 6
    RFCTYPE_FLOAT = 7
    RFCTYPE_INT = 8
    RFCTYPE_INT2 = 9
    RFCTYPE_INT1 = 10
    RFCTYPE_NULL = 14
    RFCTYPE_ABAPOBJECT = 16
    RFCTYPE_STRUCTURE = 17
    RFCTYPE_DECF16 = 23
    RFCTYPE_DECF34 = 24
    RFCTYPE_XMLDATA = 28
    RFCTYPE_STRING = 29
    RFCTYPE_XSTRING = 30
    RFCTYPE_BOX = 31
    RFCTYPE_GENERIC_BOX = 32
 
    #-RFC_UNIT_STATE - Processing status of a background unit---------------
    RFC_UNIT_NOT_FOUND = 0
    RFC_UNIT_IN_PROCESS = 1
    RFC_UNIT_COMMITTED = 2
    RFC_UNIT_ROLLED_BACK = 3
    RFC_UNIT_CONFIRMED = 4
 
    #-RFC_CALL_TYPE - Type of an incoming function call---------------------
    RFC_SYNCHRONOUS = 0
    RFC_TRANSACTIONAL = 1
    RFC_QUEUED = 2
    RFC_BACKGROUND_UNIT = 3
 
    #-RFC_DIRECTION - Direction of a function module parameter--------------
    RFC_IMPORT = 1
    RFC_EXPORT = 2
    RFC_CHANGING = RFC_IMPORT + RFC_EXPORT
    RFC_TABLES = 4 + RFC_CHANGING
 
    #-RFC_CLASS_ATTRIBUTE_TYPE - Type of an ABAP object attribute-----------
    RFC_CLASS_ATTRIBUTE_INSTANCE = 0
    RFC_CLASS_ATTRIBUTE_CLASS = 1
    RFC_CLASS_ATTRIBUTE_CONSTANT = 2
 
    #-RFC_METADATA_OBJ_TYPE - Ingroup repository----------------------------
    RFC_METADATA_FUNCTION = 0
    RFC_METADATA_TYPE = 1
    RFC_METADATA_CLASS = 2
 
 
    #-Variables-------------------------------------------------------------
    ErrInf = RFC_ERROR_INFO; RfcErrInf = ErrInf()
    ConnParams = RFC_CONNECTION_PARAMETER * 5; RfcConnParams = ConnParams()
    SConParams = RFC_CONNECTION_PARAMETER * 3; RfcSConParams = SConParams()
 
 
    #-Library---------------------------------------------------------------
    # if str(platform.architecture()[0]) == "32bit":
    #   os.environ['PATH'] += ";C:\\SAPRFCSDK\\32bit"
    #   SAPNWRFC = "C:\\SAPRFCSDK\\32bit\\sapnwrfc.dll"
    # elif str(platform.architecture()[0]) == "64bit":
    #   os.environ['PATH'] += ";C:\\SAPRFCSDK\\64bit"
    #   SAPNWRFC = "C:\\SAPRFCSDK\\64bit\\sapnwrfc.dll"
 
    SAPNWRFC = "sapnwrfc.dll"
    SAP = windll.LoadLibrary(SAPNWRFC)
 
    #-Prototypes------------------------------------------------------------
    SAP.RfcAppendNewRow.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcAppendNewRow.restype = c_void_p
 
    SAP.RfcCreateTable.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcCreateTable.restype = c_void_p
 
    SAP.RfcCloseConnection.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcCloseConnection.restype = c_ulong
 
    SAP.RfcCreateFunction.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcCreateFunction.restype = c_void_p
 
    SAP.RfcCreateFunctionDesc.argtypes = [c_wchar_p, POINTER(ErrInf)]
    SAP.RfcCreateFunctionDesc.restype = c_void_p
 
    SAP.RfcDestroyFunction.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcDestroyFunction.restype = c_ulong
 
    SAP.RfcDestroyFunctionDesc.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcDestroyFunctionDesc.restype = c_ulong
 
    SAP.RfcGetChars.argtypes = [c_void_p, c_wchar_p, c_void_p, c_ulong, \
    POINTER(ErrInf)]
    SAP.RfcGetChars.restype = c_ulong
 
    SAP.RfcGetCurrentRow.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcGetCurrentRow.restype = c_void_p
 
    SAP.RfcGetFunctionDesc.argtypes = [c_void_p, c_wchar_p, POINTER(ErrInf)]
    SAP.RfcGetFunctionDesc.restype = c_void_p
 
    SAP.RfcGetRowCount.argtypes = [c_void_p, POINTER(c_ulong), \
    POINTER(ErrInf)]
    SAP.RfcGetRowCount.restype = c_ulong
 
    SAP.RfcGetStructure.argtypes = [c_void_p, c_wchar_p, \
    POINTER(c_void_p), POINTER(ErrInf)]
    SAP.RfcGetStructure.restype = c_ulong
 
    SAP.RfcGetTable.argtypes = [c_void_p, c_wchar_p, POINTER(c_void_p), \
    POINTER(ErrInf)]
    SAP.RfcGetTable.restype = c_ulong
 
    SAP.RfcGetVersion.argtypes = [POINTER(c_ulong), POINTER(c_ulong), \
    POINTER(c_ulong)]
    SAP.RfcGetVersion.restype = c_wchar_p
 
    SAP.RfcInstallServerFunction.argtypes = [c_wchar_p, c_void_p, \
    c_void_p, POINTER(ErrInf)]
    SAP.RfcInstallServerFunction.restype = c_ulong
 
    SAP.RfcInvoke.argtypes = [c_void_p, c_void_p, POINTER(ErrInf)]
    SAP.RfcInvoke.restype = c_ulong
 
    SAP.RfcListenAndDispatch.argtypes = [c_void_p, c_ulong, POINTER(ErrInf)]
    SAP.RfcListenAndDispatch.restype = c_ulong
 
    SAP.RfcMoveToFirstRow.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcMoveToFirstRow.restype = c_ulong
 
    SAP.RfcMoveToNextRow.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcMoveToNextRow.restype = c_ulong
 
    SAP.RfcOpenConnection.argtypes = [POINTER(ConnParams), c_ulong, \
    POINTER(ErrInf)]
    SAP.RfcOpenConnection.restype = c_void_p
 
    SAP.RfcPing.argtypes = [c_void_p, POINTER(ErrInf)]
    SAP.RfcPing.restype = c_ulong
 
    SAP.RfcRegisterServer.argtypes = [POINTER(SConParams), c_ulong, \
    POINTER(ErrInf)]
    SAP.RfcRegisterServer.restype = c_void_p
 
    SAP.RfcSetChars.argtypes = [c_void_p, c_wchar_p, c_wchar_p, c_ulong, \
    POINTER(ErrInf)]
    SAP.RfcSetChars.restype = c_ulong
   
 
    #-Main------------------------------------------------------------------
 
    RfcConnParams[0].name = "ASHOST"; RfcConnParams[0].value = "34.194.191.113"
    RfcConnParams[1].name = "SYSNR" ; RfcConnParams[1].value = "01"
    RfcConnParams[2].name = "CLIENT"; RfcConnParams[2].value = "100"
    RfcConnParams[3].name = "USER"  ; RfcConnParams[3].value = "RAJKUMARS"
    RfcConnParams[4].name = "PASSWD"; RfcConnParams[4].value = "JaiHanuman10"
 
    TableName = "MARC"
    keyFields = []
    cnt = 0
 
    hRFC = SAP.RfcOpenConnection(RfcConnParams, 5, RfcErrInf)
    if hRFC != None:
 
      charBuffer = create_unicode_buffer(1048576 + 1)
 
      hFuncDesc = SAP.RfcGetFunctionDesc(hRFC, "CACS_GET_TABLE_FIELD450", RfcErrInf)
      if hFuncDesc != 0:
        hFunc = SAP.RfcCreateFunction(hFuncDesc, RfcErrInf)
        if hFunc != 0:
 
          rc = SAP.RfcSetChars(hFunc, "I_TABNAME", TableName, \
            len(TableName), RfcErrInf)
          print(SAP.RfcInvoke(hRFC, hFunc, RfcErrInf) == RFC_OK)
          if SAP.RfcInvoke(hRFC, hFunc, RfcErrInf) == RFC_OK:
 
            hTable = c_void_p(0)
            if SAP.RfcGetTable(hFunc, "T_KEYFIELD", hTable, RfcErrInf) == RFC_OK:
              RowCount = c_ulong(0)
              rc = SAP.RfcGetRowCount(hTable, RowCount, RfcErrInf)
              print(RowCount, 1)
              rc = SAP.RfcMoveToFirstRow(hTable, RfcErrInf)
              for i in range(0, RowCount.value):
                hRow = SAP.RfcGetCurrentRow(hTable, RfcErrInf)
                rc = SAP.RfcGetChars(hRow, "FIELDNAME", charBuffer, 512, RfcErrInf)
                # print(str(charBuffer.value), end="    ")
                fieldName = str(charBuffer.value)
                # rc = SAP.RfcGetChars(hRow, "LENGTH", charBuffer, 512, RfcErrInf)
                # val = int(charBuffer.value)
                # if (sum + val < 512):
                #   sum += val
                #   l1.append(fieldName.strip())
                #   # print(sum)
                # else:
                keyFields.append(fieldName.strip())
                  # l1 = [fieldName.strip()]
                  # sum = val
                if i < RowCount.value:
                  rc = SAP.RfcMoveToNextRow(hTable, RfcErrInf)
 
          rc = SAP.RfcDestroyFunction(hFunc, RfcErrInf)
 
      # rc = SAP.RfcCloseConnection(hRFC, RfcErrInf)
 
      print(*keyFields)
      keyFieldsCnt = len(keyFields)
      print(keyFieldsCnt)
    else:
      print(RfcErrInf.key)
      print(RfcErrInf.message)
 
 
    ind, keyDict = 0, {}
 
    # hRFC = SAP.RfcOpenConnection(RfcConnParams, 5, RfcErrInf)
    if hRFC != None:
 
      charBuffer = create_unicode_buffer(1048576 + 1)
 
      hFuncDesc = SAP.RfcGetFunctionDesc(hRFC, "Z450RFC_READ_TABLE", RfcErrInf)
      if hFuncDesc != 0:
        hFunc = SAP.RfcCreateFunction(hFuncDesc, RfcErrInf)
        if hFunc != 0:
 
          rc = SAP.RfcSetChars(hFunc, "QUERY_TABLE", TableName, \
            len(TableName), RfcErrInf)
          rc = SAP.RfcSetChars(hFunc, "DELIMITER", "~", 1, RfcErrInf)
          if SAP.RfcInvoke(hRFC, hFunc, RfcErrInf) == RFC_OK:
 
            hTable = c_void_p(0)
            if SAP.RfcGetTable(hFunc, "FIELDS", hTable, RfcErrInf) == RFC_OK:
             
             
              sum, l, l1 = 0, [], keyFields.copy()
              keyFieldsLen = 0
              RowCount = c_ulong(0)
              rc = SAP.RfcGetRowCount(hTable, RowCount, RfcErrInf)
              print(RowCount)
              rc = SAP.RfcMoveToFirstRow(hTable, RfcErrInf)
              for i in range(0, RowCount.value):
                hRow = SAP.RfcGetCurrentRow(hTable, RfcErrInf)
                rc = SAP.RfcGetChars(hRow, "FIELDNAME", charBuffer, 512, RfcErrInf)
                # print(str(charBuffer.value), end="    ")
                fieldName = str(charBuffer.value)
                rc = SAP.RfcGetChars(hRow, "LENGTH", charBuffer, 512, RfcErrInf)
                val = int(charBuffer.value)
                cnt += 1
                # print(fieldName.strip(), cnt)
                if (i < keyFieldsCnt):
                  print(i)
                  i += 1
                  keyFieldsLen += val
                else:
                  if (sum + val + keyFieldsLen < 400):
                    sum += val
                    l1.append(fieldName.strip())
                    # print(sum)
                  else:
                    l.append(l1)
                    l1 = keyFields.copy()
                    l1.append(fieldName.strip())
                    # print(sum + keyFieldsLen)
                    sum = val
                   
                if i < RowCount.value:
                  rc = SAP.RfcMoveToNextRow(hTable, RfcErrInf)
              l.append(l1)
          rc = SAP.RfcDestroyFunction(hFunc, RfcErrInf)
 
      # rc = SAP.RfcCloseConnection(hRFC, RfcErrInf)
 
      # print(l)
    else:
      print(RfcErrInf.key)
      print(RfcErrInf.message)
 
    # for i in l:
    #   print(i[:2])
 
    length = 0
    for ii in l:
      for jj in ii:
        if (jj == 'USERNAME'):
            length += 1
    print(l)
    print("Total Number of  : ",length)
 
    jsonTemp =[]
    jsonPrimary = []
    for splittedFields in l:
      # hRFC = SAP.RfcOpenConnection(RfcConnParams, 5, RfcErrInf)
      if hRFC != None:
 
        charBuffer = create_unicode_buffer(1048576 + 1)
 
        hFuncDesc = SAP.RfcGetFunctionDesc(hRFC, "Z450RFC_READ_TAB_DATA", RfcErrInf)
        if hFuncDesc != 0:
          hFunc = SAP.RfcCreateFunction(hFuncDesc, RfcErrInf)
          if hFunc != 0:
 
            rc = SAP.RfcSetChars(hFunc, "QUERY_TABLE", TableName, \
              len(TableName), RfcErrInf)
            rc = SAP.RfcSetChars(hFunc, "DELIMITER", "~", 1, RfcErrInf)
 
            #MATNR,MTART,ATTYP,SATNR,MATKL,MBRSH,MEINS,SPART,BISMT,DATAB,LIQDT,NORMT,GROES,LABOR,BRGEW,NTGEW,GEWEI,LAENG,BREIT,HOEHE,MEABM,VOLUM,VOLEH,KZKFG,IPRKZ,RAUBE,TEMPB,BEHVO,STOFF,ETIAR,ETIFO,WESCH,XGCHP,MHDHB,MHDRZ,SLED_BBD
 
            field = ','.join(splittedFields)
            # print(field)
            rc = SAP.RfcSetChars(hFunc, "FIELDNAME", field, len(field), RfcErrInf)
 
            # print(SAP.RfcInvoke(hRFC, hFunc, RfcErrInf) == RFC_OK)
            if SAP.RfcInvoke(hRFC, hFunc, RfcErrInf) == RFC_OK:
 
              hTable = c_void_p(0)
              if SAP.RfcGetTable(hFunc, "DATA", hTable, RfcErrInf) == RFC_OK:
 
                RowCount = c_ulong(0)
                rc = SAP.RfcGetRowCount(hTable, RowCount, RfcErrInf)
                rc = SAP.RfcMoveToFirstRow(hTable, RfcErrInf)
                for i in range(0, RowCount.value):
                  hRow = SAP.RfcGetCurrentRow(hTable, RfcErrInf)
                  rc = SAP.RfcGetChars(hRow, "WA", charBuffer, 1024, RfcErrInf)
                  data_row=charBuffer.value
                  # data_dict = {field: value for field, value in zip(splittedFields, data_row)}
                  # print(data_dict)
 
                  data_row = charBuffer.value.split("~")
                #   print(data_row)
   
                                    # Create dictionary using only requested fields
                                # data_dict = {field: value for field, value in zip(field, data_row)}
                                # # print(charBuffer.value)
                                # res.append(data_dict)
                  fields = field.split(",")
                  data_dict = {f: v.strip() for f, v in zip(fields, data_row)}
                  jsonTemp.append(data_dict)
                #   print(jsonTemp)
                #   print("Hiieloooooooooooooooooooooooooooooooooooooo")
 
                  if i < RowCount.value:
                    rc = SAP.RfcMoveToNextRow(hTable, RfcErrInf)
 
            rc = SAP.RfcDestroyFunction(hFunc, RfcErrInf)
        print(len(jsonPrimary))
        if (len(jsonPrimary) == 0):
        #   print("HEYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY")
          jsonPrimary = jsonTemp
        else:
        #   print(jsonPrimary)
        #   print("YASHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH")
          jsonPrimary = join_json_objects_multiple_keys(jsonPrimary, jsonTemp, keyFields)
        jsonTemp = []
        # rc = SAP.RfcCloseConnection(hRFC, RfcErrInf)
   
 
      else:
        print(RfcErrInf.key)
        print(RfcErrInf.message)
 
 
 
    df = pd.DataFrame(jsonPrimary)
    # print(df)
    new_column_list = df.columns.tolist()
    # print(column_list)
 
    # Create a new list to store the modified column names
    column_list = []
    for cl in new_column_list:
        if cl == "PRIMARY":
            column_list.append("PRIMARY1")
        else:
            column_list.append(cl)
 
    df.columns = column_list
    columns = []
    col =[]
 
    for cl in column_list:
        col.append(cl)
        col.append("TEXT")
        columns.append(col)
        col=[]
    create_table(TableName, columns)
    insert_data_from_dataframe(df,TableName)
    # print("Final JSON : ",jsonPrimary)
    return Response(jsonPrimary)
 
 
    del SAP
 
    # for d in jsonPrimary:
    #   for j in d:
    #     d[j] = d[j].strip()
 
    # print(jsonPrimary[-1])
 
 