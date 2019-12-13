import pyodbc
import pprint
import datetime
import decimal
import os
import re


# Source

srcDB = "[CRM_ODS].[dbo]"
tgtDB = "hive.customer_modeled"
#tgtDB = "hive.raghu_format"
pk = "customer_id"

global logFile 
global dFile 


def openfiles(filename):
    global logFile
    global dFile

    ts=datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if not os.path.exists("log"):
        os.makedirs("log")
    if not os.path.exists("out"):
        os.makedirs("out")

    logFile=open("log/" + filename + "_" + ts + ".log","w")
    dFile=open("out/" + filename + "_" + ts + ".csv","w",encoding="utf-8")

while True:
    prmFile=input("Enter Parameter File name : ")
    if not os.path.exists(prmFile):
        print("File " + os.path +"/" + prmFile + "Does not exisit")
    else:
        pFile=open(prmFile,"r")
        srcDSN=re.sub('\n','',re.sub("SRCDSN:",'',pFile.readline()))
        tgtDSN=re.sub('\n','',re.sub("TGTDSN:",'',pFile.readline()))
        if srcDSN=="":
            print("Config file incorrect. Line 1 does not have SRCDSN: parameter")
            continue
        elif tgtDSN=="":
            print("Config file incorrect. Line 2 does not have TGTDSN: parameter")
            continue
        break

logFile=""
dFile=""

tcnn=pyodbc.connect("DSN="+tgtDSN+";UID=secure.jraghuraman;PWD=Arugee0714*;",autocommit=True);
scnn = pyodbc.connect("Driver=ODBC Driver 17 for SQL Server;Server=FL1DVMDBADB02; Port=1433;Database=CRM_ODS;UID=crm_ddi;PWD=G5lGlDztAortaunARhqN;")
dcnn = pyodbc.connect("Driver=ODBC Driver 17 for SQL Server;Server=FL1DVMDBADB02; Port=1433;Database=CRM_ODS;UID=crm_ddi;PWD=G5lGlDztAortaunARhqN;")

lsrcSql=[None]*20
ltgtSql=[None]*20
tbl=[None]*20
fullTbl=[None]*20
tcur=tcnn.cursor();

topn=400000
                    
tbl[1]="Customer"
tgtSql="select * from " + tgtDB + "." + tbl[1] + " limit 1"
tcur.execute(tgtSql)
sOutput=""
for i  in range(0, len(tcur.description)):
    if tcur.description[i][0].upper().find("UPDATED_ON")==-1 and tcur.description[i][0].upper().find("UPDATED_DATE")==-1 and tcur.description[i][0].upper().find("DATE_LAST_MODIFIED")==-1:
        sOutput+=tcur.description[i][0]+","
flds=sOutput[0:-1]

lsrcSql[1]="select Top " + str(topn) + " " + flds + ", " + pk + " pkey from " + srcDB + "." + tbl[1] + " where  customer_id>{} order by " + pk 
ltgtSql[1]="select " + flds + ", " + pk + " pkey from " + tgtDB + "." + tbl[1] + " where  customer_id>{} order by " + pk + " limit " + str(topn)
fullTbl[1]="N"
tbl[2]="customer_contact_mail_pii"

tgtSql="select  * from " + tgtDB + "." + tbl[2] + " B limit 1" 
tcur.execute(tgtSql)
sOutput=""
for i  in range(0, len(tcur.description)):
    if tcur.description[i][0].upper().find("UPDATED_ON")==-1:
        sOutput+=tcur.description[i][0]+","
flds=sOutput[0:-1]

#lsrcSql[2]="select " + flds + ",customer_mail_id pkey  from " + srcDB + "." + tbl[2] + " B Join " + srcDB + "." + tbl[1] + " A on (A." +pk + "=" + "B." + pk+")  where   customer_mail_id in (613, 2006) order by pkey"
#ltgtSql[2]="select " + flds + ",customer_mail_id pkey  from " + tgtDB + "." + tbl[2] + " B Join " + tgtDB + "." + tbl[1] + " A on (A." +pk + "=" + "B." + pk+")  where   customer_mail_id in (613, 2006) order by pkey"

lsrcSql[2]="select Top " + str(topn) + " " + flds + ", customer_mail_id pkey  from " + srcDB + "." + tbl[2] + " B where  customer_mail_id>{}  order by customer_mail_id"
ltgtSql[2]="select " + flds + ", customer_mail_id pkey from " + tgtDB + "." + tbl[2] + " B where  customer_mail_id>{}  order by customer_mail_id Limit " + str(topn)
fullTbl[2]="Y"

tbl[3]="customer_contact_email_pii"

tgtSql="select * from " + tgtDB + "." + tbl[3] + " B limit 1" 
tcur.execute(tgtSql)
sOutput=""
for i  in range(0, len(tcur.description)):
    if tcur.description[i][0].upper().find("UPDATED_ON")==-1:
        sOutput+=tcur.description[i][0]+","
flds=sOutput[0:-1]

lsrcSql[3]="select Top " + str(topn) + " " +  flds + ", customer_email_id  pkey from " + srcDB + "." + tbl[3] + " B where customer_email_id>{} order by customer_email_id"
ltgtSql[3]="select " + flds + ", customer_email_id  pkey from " + tgtDB + "." + tbl[3] + " B  where  customer_email_id>{}  order by customer_email_id Limit " + str(topn)
fullTbl[3]="Y"

tbl[4]="customer_contact_phone_pii"

tgtSql="select  * from " + tgtDB + "." + tbl[4] + " B limit 1" 
tcur.execute(tgtSql)
sOutput=""
for i  in range(0, len(tcur.description)):
    if tcur.description[i][0].upper().find("UPDATED_ON")==-1:
        sOutput+=tcur.description[i][0]+","
flds=sOutput[0:-1]
lsrcSql[4]="select Top " + str(topn) + " "  + flds + ",customer_phone_id  pkey from " + srcDB + "." + tbl[4] + " B where  customer_phone_id>{} order by customer_phone_id"
ltgtSql[4]="select " + flds + ",customer_phone_id  pkey from " + tgtDB + "." + tbl[4] + " B where  customer_phone_id>{} order by customer_phone_id Limit " + str(topn)
fullTbl[4]="Y"

tbl[5]="alternate_key"

tgtSql="select  * from " + tgtDB + "." + tbl[5] + " limit 1" 
tcur.execute(tgtSql)
sOutput=""
for i  in range(0, len(tcur.description)):
    if  tcur.description[i][0].upper().find("UPDATED_ON")==-1:
        sOutput+=tcur.description[i][0]+","
flds=sOutput[0:-1]

lsrcSql[5]="select " + flds + ", alternate_key_id  pkey from " + srcDB + "." + tbl[5] + " where alternate_key_id>{} order by alternate_key_id"
ltgtSql[5]="select " + flds + ", alternate_key_id  pkey  from " + tgtDB + "." + tbl[5] + " where alternate_key_id>{} order by alternate_key_id"
fullTbl[5]="Y"

tbl[6]="campaign"
tgtSql="select  * from " + tgtDB + "." + tbl[6] + " limit 1" 
tcur.execute(tgtSql)
sOutput=""
for i  in range(0, len(tcur.description)):
    if tcur.description[i][0].upper().find("UPDATED_ON")==-1:
        sOutput+=tcur.description[i][0]+","
flds=sOutput[0:-1]

lsrcSql[6]="select " + flds + ",campaign_id  pkey from " + srcDB + "." + tbl[6] + " where campaign_id>{} order by campaign_id"
ltgtSql[6]="select " + flds + ",campaign_id  pkey from " + tgtDB + "." + tbl[6] + " where campaign_id>{} order by campaign_id"
fullTbl[6]="Y"

ctr=7
tbl[ctr]="coupon"
tgtSql="select * from " + tgtDB + "." + tbl[ctr] + "  limit 1" 
tcur.execute(tgtSql)
sOutput=""
for i  in range(0, len(tcur.description)):
    if tcur.description[i][0].upper().find("UPDATED_ON")==-1:
        sOutput+=tcur.description[i][0]+","
flds=sOutput[0:-1]

lsrcSql[ctr]="select " + flds + ",coupon_id  pkey from " + srcDB + "." + tbl[ctr] + " where coupon_id>{} order by coupon_id"
ltgtSql[ctr]="select " + flds + ",coupon_id  pkey from " + tgtDB + "." + tbl[ctr] + " where coupon_id>{} order by coupon_id"
fullTbl[7]="Y"

ctr=8
tbl[ctr]="create_source"
tgtSql="select * from " + tgtDB + "." + tbl[ctr] + "  limit 1" 
tcur.execute(tgtSql)
sOutput=""
for i  in range(0, len(tcur.description)):
    if tcur.description[i][0].upper().find("UPDATED_ON")==-1:
        sOutput+=tcur.description[i][0]+","
flds=sOutput[0:-1]
lsrcSql[ctr]="select " + flds + ",create_source_id  pkey from " + srcDB + "." + tbl[ctr] + " where create_source_id>{} order by create_source_id"
ltgtSql[ctr]="select " + flds + ",create_source_id  pkey from " + tgtDB + "." + tbl[ctr] + " where create_source_id>{} order by create_source_id"
fullTbl[ctr]="Y"

ctr=9
tbl[ctr]="customer_coupon"
tgtSql="select * from " + tgtDB + "." + tbl[ctr] + "  limit 1" 
tcur.execute(tgtSql)
sOutput=""
for i  in range(0, len(tcur.description)):
    if tcur.description[i][0].upper().find("UPDATED_ON")==-1:
        sOutput+=tcur.description[i][0]+","
flds=sOutput[0:-1]
lsrcSql[ctr]="select " + flds + ",customer_coupon_id  pkey from " + srcDB + "." + tbl[ctr] + " where customer_coupon_id>{} order by customer_coupon_id"
ltgtSql[ctr]="select " + flds + ",customer_coupon_id  pkey from " + tgtDB + "." + tbl[ctr] + " where customer_coupon_id>{} order by customer_coupon_id"
fullTbl[ctr]="Y"

ctr=10
tbl[ctr]="customer_event"

tgtSql="select  * from " + tgtDB + "." + tbl[ctr] + " B limit 1" 
tcur.execute(tgtSql)
sOutput=""
for i  in range(0, len(tcur.description)):
    if tcur.description[i][0].upper().find("UPDATED_ON")==-1:
        sOutput+=tcur.description[i][0]+","
flds=sOutput[0:-1]

lsrcSql[ctr]="select Top " + str(topn) + " " + flds + ",customer_event_id  pkey from " + srcDB + "." + tbl[ctr] + " B where  customer_event_id>{} order by customer_event_id"
ltgtSql[ctr]="select " + flds + ",customer_event_id  pkey from " + tgtDB + "." + tbl[ctr] + " B where  customer_event_id>{}  order by customer_event_id Limit " + str(topn)
fullTbl[ctr]="Y"

ctr=11
tbl[ctr]="customer_loyalty"
#lsrcSql[ctr]="select customer_loyalty_id  pkey, B.* from " + srcDB + "." + tbl[ctr] + " B Join " + srcDB + "." + tbl[1] + " A on (A." +pk + "=" + "B." + pk+")  where  cast(A.date_last_modified as date)='{}' order by customer_loyalty_id"
#ltgtSql[ctr]="select customer_loyalty_id pkey, B.* from " + tgtDB + "." + tbl[ctr] + " B Join " + tgtDB + "." + tbl[1] + " A on (A." +pk + "=" + "B." + pk+")  where  cast(A.date_last_modified as date)='{}' order by customer_loyalty_id"
lsrcSql[ctr]="select Top " + str(topn) + " CUSTOMER_LOYALTY_ID, B.CUSTOMER_ID ,LOYALTY_TYPE_ID ,IS_ACTIVE ,LOYALTY_DATE ,LOYALTY_FLAG ,LIFETIME_SPEND ,SPEND_DATE ,B.DATE_LAST_MODIFIED, LOYALTY_TIER ,LOYALTY_TIER_DATE, LOYALTY_SPEND ,LOYALTY_DELTA ,LOYALTY_OPTIN_DATE ,AVAILABLE_POINTS,customer_loyalty_id  pkey  from " + srcDB + "." + tbl[ctr] + " B where  customer_loyalty_id>{}  order by customer_loyalty_id"
ltgtSql[ctr]="select CUSTOMER_LOYALTY_ID, B.CUSTOMER_ID ,LOYALTY_TYPE_ID ,IS_ACTIVE ,LOYALTY_DATE ,LOYALTY_FLAG ,LIFETIME_SPEND ,SPEND_DATE ,B.DATE_LAST_MODIFIED, LOYALTY_TIER ,LOYALTY_TIER_DATE, LOYALTY_SPEND ,LOYALTY_DELTA ,LOYALTY_OPTIN_DATE ,AVAILABLE_POINTS,customer_loyalty_id  pkey from " + tgtDB + "." + tbl[ctr] + " B where  customer_loyalty_id>{} order by customer_loyalty_id limit " + str(topn) 
fullTbl[ctr]="Y"

ctr=12
tbl[ctr]="customer_third_party"

tgtSql="select * from " + tgtDB + "." + tbl[ctr] + "  limit 1" 
tcur.execute(tgtSql)
sOutput=""
for i  in range(0, len(tcur.description)):
    if  tcur.description[i][0].upper().find("UPDATED_ON")==-1:
        sOutput+=tcur.description[i][0]+","
flds=sOutput[0:-1]

lsrcSql[ctr]="select " + flds + ",customer_tp_id  pkey from " + srcDB + "." + tbl[ctr] + " where customer_tp_id>{} order by customer_tp_id"
ltgtSql[ctr]="select " + flds + ",customer_tp_id  pkey from " + tgtDB + "." + tbl[ctr] + " where customer_tp_id>{} order by customer_tp_id"
fullTbl[ctr]="Y"

ctr=13
tbl[ctr]="customer_xref"
tgtSql="select  * from " + tgtDB + "." + tbl[ctr] + "  limit 1" 
tcur.execute(tgtSql)
sOutput=""
for i  in range(0, len(tcur.description)):
    if tcur.description[i][0].upper().find("UPDATED_ON")==-1:
        sOutput+=tcur.description[i][0]+","
flds=sOutput[0:-1]

lsrcSql[ctr]="select " + flds + ",customer_xref_id  pkey from " + srcDB + "." + tbl[ctr] + " where customer_xref_id>{} order by customer_xref_id"
ltgtSql[ctr]="select " + flds + ",customer_xref_id  pkey from " + tgtDB + "." + tbl[ctr] + " where customer_xref_id>{} order by customer_xref_id"
fullTbl[ctr]="Y"

ctr=14
tbl[ctr]="event_type"
tgtSql="select * from " + tgtDB + "." + tbl[ctr] + "  limit 1" 
tcur.execute(tgtSql)
sOutput=""
for i  in range(0, len(tcur.description)):
    if tcur.description[i][0].upper().find("UPDATED_ON")==-1:
        sOutput+=tcur.description[i][0]+","
flds=sOutput[0:-1]
lsrcSql[ctr]="select " + flds + ",event_type_id  pkey from " + srcDB + "." + tbl[ctr] + " where event_type_id>{} order by event_type_id"
ltgtSql[ctr]="select " + flds + ",event_type_id  pkey from " + tgtDB + "." + tbl[ctr] + " where event_type_id>{} order by event_type_id"
fullTbl[ctr]="Y"

ctr=15
tbl[ctr]="preference_type"
tgtSql="select * from " + tgtDB + "." + tbl[ctr] + "  limit 1" 
tcur.execute(tgtSql)
sOutput=""
for i  in range(0, len(tcur.description)):
    if tcur.description[i][0].upper().find("UPDATED_ON")==-1:
        sOutput+=tcur.description[i][0]+","
flds=sOutput[0:-1]
lsrcSql[ctr]="select " + flds + ",preference_type_id  pkey from " + srcDB + "." + tbl[ctr] + " where preference_type_id>{} order by preference_type_id "
ltgtSql[ctr]="select " + flds + ",preference_type_id  pkey from " + tgtDB + "." + tbl[ctr] + " where preference_type_id>{} order by preference_type_id "
fullTbl[ctr]="Y"

ctr=16
tbl[ctr]="customer_preference"
tgtSql="select * from " + tgtDB + "." + tbl[ctr] + " B limit 1" 
tcur.execute(tgtSql)
sOutput=""
for i  in range(0, len(tcur.description)):
    if tcur.description[i][0].upper().find("UPDATED_ON")==-1:
        sOutput+=tcur.description[i][0]+","
flds=sOutput[0:-1]
lsrcSql[ctr]="select Top " + str(topn) + " " + flds  + " ,customer_preference_id  pkey from " + srcDB + "." + tbl[ctr] + " B where  customer_preference_id>{} order by customer_preference_id"
ltgtSql[ctr]="select " + flds + ",customer_preference_id  pkey from " + tgtDB + "." + tbl[ctr] + " B where  customer_preference_id>{}  order by customer_preference_id limit " + str(topn)
fullTbl[ctr]="Y"

ctr=17
tbl[ctr]="event_group"
tgtSql="select * from " + tgtDB + "." + tbl[ctr] + "  limit 1" 
tcur.execute(tgtSql)
sOutput=""
for i  in range(0, len(tcur.description)):
    if tcur.description[i][0].upper().find("UPDATED_ON")==-1:
        sOutput+=tcur.description[i][0]+","
flds=sOutput[0:-1]
lsrcSql[ctr]="select " + flds + ",event_group_id  pkey from " + srcDB + "." + tbl[ctr] + " where event_group_id>{} order by event_group_id "
ltgtSql[ctr]="select " + flds + ",event_group_id  pkey from " + tgtDB + "." + tbl[ctr] + " where event_group_id>{} order by event_group_id "
fullTbl[ctr]="Y"

ctr=18
tbl[ctr]="exp_customer_xref"
tgtSql="select  * from " + tgtDB + "." + tbl[ctr] + "  limit 1" 
tcur.execute(tgtSql)
sOutput=""
for i  in range(0, len(tcur.description)):
    if tcur.description[i][0].upper().find("UPDATED_ON")==-1:
        sOutput+=tcur.description[i][0]+","
flds=sOutput[0:-1]
lsrcSql[ctr]="select " + flds + ",rtrim(cast(brand_id as char(1))+'-'+cast(customer_no as char(20))) pkey from " + srcDB + "." + tbl[ctr] + " order by rtrim(cast(brand_id as char(1))+'-'+cast(customer_no as char(20)))"
ltgtSql[ctr]="select " + flds + ",brand_id||'-'||customer_no pkey from " + tgtDB + "." + tbl[ctr] + " order by brand_id||'-'||customer_no"
fullTbl[ctr]="Y"

ctr=19
tbl[ctr]="preference_group"
tgtSql="select  * from " + tgtDB + "." + tbl[ctr] + "  limit 1" 
tcur.execute(tgtSql)
sOutput=""
for i  in range(0, len(tcur.description)):
    if tcur.description[i][0].upper().find("UPDATED_ON")==-1:
        sOutput+=tcur.description[i][0]+","
flds=sOutput[0:-1]
lsrcSql[ctr]="select " + flds + ",preference_group_id  pkey from " + srcDB + "." + tbl[ctr] + " where preference_group_id>{} order by preference_group_id "
ltgtSql[ctr]="select " + flds + ",preference_group_id  pkey from " + tgtDB + "." + tbl[ctr] + " where preference_group_id>{} order by preference_group_id  "
fullTbl[ctr]="Y"

#for nDay in range(1,20):
#    print (lsrcSql[i])

for nDay in range(1,20):
    daysCur=dcnn.cursor()
    neverStop="N"
    srcRowCnt=0
    tgtRowCnt=0
    rowMismatch=0
    notInSrc=0
    notInTgt=0
    flderrcnt=list()
    openfiles(tbl[nDay]) 
    logFile.write("[I] {}  Validation started\n".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
    dctr=0
    vCustomer_id=-1
        
    while neverStop=="N":
        dctr=dctr+1
        if dctr>1 and nDay==18:
            neverStop="Y"
            continue
        srcSql=lsrcSql[nDay].format(vCustomer_id)  
        tgtSql=ltgtSql[nDay].format(vCustomer_id)
        print(srcSql,"\n",tgtSql)
        logFile.write("[I] {} Source SQL {}  \n".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), srcSql))
        logFile.write("[I] {} Target SQL {}  \n".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), tgtSql))
        print("[I]  **** Validating " + tbl[nDay] + " " + str(vCustomer_id) + "...")
        
        scur=scnn.cursor();
        scur.execute(srcSql)

        tcur=tcnn.cursor();
        tcur.execute(tgtSql)


        srcRow=scur.fetchone()
        if srcRow==None:
            neverStop="Y"
            continue
        tgtRow=tcur.fetchone()
        keyFld=len(scur.description)-1
        while True:
            if srcRow!=None:
                vCustomer_id=srcRow[keyFld]
            dFile.flush()
            logFile.flush()
            if (srcRowCnt%1000==0):
                print("Verified {} rows.".format(srcRowCnt))
            #print(srcRow ,tgtRow )
            if not srcRow and not tgtRow:
                break
            if not srcRow:
                print("srcRow EOF")
                sOutput=str(tgtRow[0]) + " not found in Source"
                notInSrc+=1
                dFile.write(sOutput + "\n")
                tgtRow=tcur.fetchone()
                srcRowCnt+=1
                tgtRowCnt+=1
                continue
          
            if not tgtRow:
                print("srcRow EOF")
                sOutput=str(srcRow[0]) + " not found in Target"
                notInTgt+=1
                dFile.write(sOutput + "\n")
                srcRow=scur.fetchone()
                srcRowCnt+=1
                tgtRowCnt+=1
                continue
               
            if srcRowCnt==0:
                sOutput=""
                fldlist={}
                errStructure="N"
                for i  in range(0, len(scur.description)-1):
                    srcDesc=scur.description[i]
                    tgtDesc=tcur.description[i]
                    #print(srcDesc[0].upper()!=tgtDesc[0].upper(),srcDesc[0].upper(),tgtDesc[0].upper())
                    sOutput+=srcDesc[0]+","
                    fldlist[srcDesc[0]] = i
                    flderrcnt.append(0)
                sOutput="PKEY,"+sOutput[0:-1]
                hdr="PKEY,"+sOutput
                if errStructure=="1":
                    print("[E] **** ERROR in structure for table "+tbl[nDay])
                    dFile.write(sOutput+"\n")
                    dFile.write("[E] **** ERROR in structure did not validate\n")
                    break
                dFile.write(sOutput+"\n")
                
            if srcRow[keyFld]==tgtRow[keyFld]:
                #print("In =")
                # Row Matched. Validate each field in src/tgt
                diff="N"
                sOutput=""
                for i in range(0, len(scur.description)-1):
                    srcDesc=scur.description[i]
                    tgtDesc=tcur.description[i]
                    if  srcRow[fldlist[srcDesc[0]]] in ("(null)","NULL","null") and tgtRow[fldlist[srcDesc[0]]]==None:
                        sOutput+=","
                        continue
                    if  tgtRow[fldlist[srcDesc[0]]]==None and len(str(srcRow[fldlist[srcDesc[0]]]).rstrip())==0:
                        sOutput+=","                
                    elif srcRow[fldlist[srcDesc[0]]]!=str(tgtRow[fldlist[srcDesc[0]]]):
                        #print(srcDesc[0],srcDesc[1],tgtDesc[0],tgtDesc[1],srcRow[fldlist[srcDesc[0]]],tgtRow[fldlist[srcDesc[0]]])
                        #a=input("Press enter")
                        if isinstance(tgtRow[fldlist[srcDesc[0]]],bool):
                            if (srcRow[fldlist[srcDesc[0]]]=="NULL" and tgtRow[fldlist[srcDesc[0]]]) or (srcRow[fldlist[srcDesc[0]]]=="1" and not tgtRow[fldlist[srcDesc[0]]]):
                                sOutput+=str(srcRow[fldlist[srcDesc[0]]])+" @ " + str(tgtRow[fldlist[srcDesc[0]]])
                                diff="Y"
                                sOutput+=","
                                flderrcnt[i]=flderrcnt[i]+1
                            else:
                                sOutput+=","                
                        elif isinstance(tgtRow[fldlist[srcDesc[0]]],datetime.datetime):
                            if srcRow[fldlist[srcDesc[0]]]!=tgtRow[fldlist[srcDesc[0]]]:
                                sOutput+=str(srcRow[fldlist[srcDesc[0]]])+" @@ " + str(tgtRow[fldlist[srcDesc[0]]])
                                sOutput+=","                
                                flderrcnt[i]=flderrcnt[i]+1
                                diff="Y"
                            else:
                                sOutput+=","
                        elif isinstance(tgtRow[fldlist[srcDesc[0]]],datetime.date):
                            if  srcRow[fldlist[srcDesc[0]]]!=tgtRow[fldlist[srcDesc[0]]]:
                                sOutput+=str(srcRow[fldlist[srcDesc[0]]])+" @@@ " + str(tgtRow[fldlist[srcDesc[0]]])
                                flderrcnt[i]=flderrcnt[i]+1
                                sOutput+=","                
                                diff="Y"
                            else:
                                sOutput+=","
                        elif isinstance(tgtRow[fldlist[srcDesc[0]]],str):
                            #print(tgtSql)
                            #print("Str <>",srcRow[0],srcRow[fldlist[srcDesc[0]]], "["+tgtRow[fldlist[srcDesc[0]]].rstrip()+"]"=="[]", srcDesc[0])
                            if srcRow[fldlist[srcDesc[0]]]==None and str(tgtRow[fldlist[srcDesc[0]]]).rstrip()=="":
                                sOutput+=","
                            elif srcRow[fldlist[srcDesc[0]]]==None and str(tgtRow[fldlist[srcDesc[0]]]).rstrip()!=None:
                                sOutput+=str(srcRow[fldlist[srcDesc[0]]])+" @@@@ " + str(tgtRow[fldlist[srcDesc[0]]])
                                flderrcnt[i]=flderrcnt[i]+1
                                sOutput+=","                
                                diff="Y"
                            elif srcRow[fldlist[srcDesc[0]]].rstrip() == tgtRow[fldlist[srcDesc[0]]].rstrip():
                                sOutput+=","
                            elif not ((srcRow[fldlist[srcDesc[0]]]=="(null)" and tgtRow[fldlist[srcDesc[0]]]==None) or (srcRow[fldlist[srcDesc[0]]]=="NULL" and tgtRow[fldlist[srcDesc[0]]]==None)):
                                sOutput+=str(srcRow[fldlist[srcDesc[0]]])+" @@@@ " + str(tgtRow[fldlist[srcDesc[0]]])
                                flderrcnt[i]=flderrcnt[i]+1
                                sOutput+=","                
                                diff="Y"
                            else:
                                sOutput+=","
                        else:
                            if srcRow[fldlist[srcDesc[0]]]==tgtRow[fldlist[srcDesc[0]]]:
                                sOutput+=","                            
                            elif not (srcRow[fldlist[srcDesc[0]]]=="(null)" and tgtRow[fldlist[srcDesc[0]]]==None):
                                sOutput+=str(srcRow[fldlist[srcDesc[0]]])+" @@@@@ " + str(tgtRow[fldlist[srcDesc[0]]])
                                flderrcnt[i]=flderrcnt[i]+1
                                sOutput+=","                
                                diff="Y"
                            else:
                                sOutput+=","    
                    else:
                        #print(str(srcRow[fldlist[srcDesc[0]]])+" = " + str(tgtRow[fldlist[srcDesc[0]]]))
                        sOutput+=","
                if diff=="Y":
                    sOutput= str(srcRow[keyFld])+","+sOutput 
                    rowMismatch+=1
                    dFile.write(sOutput + "\n")
                srcRow=scur.fetchone()
                try:
                    tgtRow=tcur.fetchone()
                except:
                    print("[E] **** Error Reading row in hive...")
                    logFile.write("Error reading row counter ->"+str(tgtRowCnt)+"\n")      
            elif srcRow[keyFld] >tgtRow[keyFld]:
                    #print("In >")
                    sOutput=str(tgtRow[keyFld]) + " not found in Source"
                    notInSrc+=1
                    #print(sOutput)
                    dFile.write(sOutput + "\n")
                    try:
                        tgtRow=tcur.fetchone()
                    except:
                        print("[E] **** Error Reading row in hive...")
                        logFile.write("Error reading row counter ->"+str(tgtRowCnt)+"\n")      
            else:
                    #print("In <")
                    sOutput=str(srcRow[keyFld]) + " not found in Target"
                    notInTgt+=1
                    dFile.write(sOutput + "\n")
                    srcRow=scur.fetchone()
                    
            srcRowCnt+=1
            tgtRowCnt+=1
     

            
    dFile.write("\n\n*** Statistics *****\nTotal Source Rows Validated : {}\n".format(srcRowCnt))
    dFile.write("Total Target Rows Validated : {}\n".format(tgtRowCnt))
    dFile.write("Mismatch Rows  : {}\n".format(rowMismatch))
    dFile.write("Missing in Source  : {}\n".format(notInSrc))
    dFile.write("Missing in Target  : {}\n".format(notInTgt))
    sOutput=""
    for i in range(0,len(flderrcnt)-1):
        sOutput+=str(flderrcnt[i])+","
    sOutput="0,"+sOutput[0:-1]
    dFile.write("\n"+hdr+"\n")
    dFile.write(sOutput+"\n")
    logFile.write("[I] {}  Validation completed\n".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
    dFile.flush()
    logFile.flush()
    dFile.close
    logFile.close
    logFile=None
    dFile=None

print("Validation Ended")

