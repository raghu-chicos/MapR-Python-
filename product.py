import pyodbc
import pprint
import datetime
import decimal
import os
import re


# Source
srcDB = "hive.PRODUCT_RAW"
tgtDB = "hive.PRODUCT_MODELED"
tbl = "product"
pk = "article"

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
    dFile=open("out/" + filename + "_" + ts + ".csv","w")
    
while True:
    #prmFile=input("Enter Parameter File name : ")
    prmFile='qa.txt'
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

openfiles("product")


logFile.write("[I] {} Product Validation started\n".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))

scnn=pyodbc.connect("DSN="+tgtDSN+";UID=secure.jraghuraman;PWD=Arugee0714*;",autocommit=True);
srcSql="select " + pk + " pkey, * from " + srcDB + "." + tbl + " order by " + pk  + " limit 1"

scur=scnn.cursor()
scur.execute(srcSql)
sOutput=""
for i  in range(0, len(scur.description)):
    if i>0:
        sOutput+=scur.description[i][0]+","
flds=sOutput[0:-1]

scur.execute("select count(*) from "+ srcDB + "." + tbl)

srow=scur.fetchone()
loopthru = int(srow[0]/40000)+1
print("Start Validation Started")
srcRowCnt=0
tgtRowCnt=0
rowMismatch=0
notInSrc=0
notInTgt=0
flderrcnt=list()

articleid=0
print("Going to loop ",loopthru)
for iLoop in range(1,loopthru+2):
#for iLoop in range(1,1+1):
    srcSql="select " + pk + " pkey, " + flds + " from " + srcDB + "." + tbl + " where article>='" + str(articleid) + "' order by " + pk  + " limit 40000"
    tgtSql="select " + pk + " pkey, " + flds + " from " + tgtDB + "." + tbl + " where article>='" + str(articleid) + "' order by " + pk  + " limit 40000"
    logFile.write("[I] {} Source SQL {}  \n".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), srcSql))
    logFile.write("[I] {} Source SQL {}  \n".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), tgtSql))

    scur=scnn.cursor();
    scur.execute(srcSql)

    tcur=scnn.cursor();
    tcur.execute(tgtSql)

    srcRow=scur.fetchone()
    tgtRow=tcur.fetchone()
    while True:
        dFile.flush()
        logFile.flush()

        if (srcRowCnt%1000==0):
            print("Verified {} rows.".format(srcRowCnt))
        #print(srcRow ,tgtRow )
        if not srcRow and srcRowCnt==0:
            sOutput=""
            fldlist={}
            for i  in range(0, len(scur.description)):
                srcDesc=scur.description[i]
                tgtDesc=tcur.description[i]
                sOutput+=srcDesc[0]+","
                fldlist[srcDesc[0]] = i
                flderrcnt.append(0)
            sOutput=sOutput[0:-1]
            dFile.write(sOutput+"\n")
            hdr=sOutput

            print("[E] **** SOURCE IS EMPTY ")
            dFile.write("[E] **** SOURCE IS EMPTY ")
            logFile.write("[E] **** SOURCE IS EMPTY ")
            break
        if not tgtRow and srcRowCnt==0:
            sOutput=""
            fldlist={}
            for i  in range(0, len(scur.description)):
                srcDesc=scur.description[i]
                tgtDesc=tcur.description[i]
                sOutput+=srcDesc[0]+","
                fldlist[srcDesc[0]] = i
                flderrcnt.append(0)
            sOutput=sOutput[0:-1]
            dFile.write(sOutput+"\n")
            hdr=sOutput
            print("[E] **** TARGET IS EMPTY ")
            dFile.write("[E] **** TARGET IS EMPTY ")
            logFile.write("[E] **** TARGET IS EMPTY ")
            break

        if not srcRow and not tgtRow:
            break

        articleid=srcRow[0]
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
            for i  in range(0, len(scur.description)):
                srcDesc=scur.description[i]
                tgtDesc=tcur.description[i]
                sOutput+=srcDesc[0]+","
                fldlist[srcDesc[0]] = i
                flderrcnt.append(0)
            sOutput=sOutput[0:-1]
            dFile.write(sOutput+"\n")
            hdr=sOutput

        if srcRow[0]==tgtRow[0]:
            #print("In =")
            # Row Matched. Validate each field in src/tgt
            diff="N"
            sOutput=""
            for i in range(0, len(scur.description)):
                srcDesc=scur.description[i]
                tgtDesc=tcur.description[i]
                if srcDesc[0] in ("product.updated_on","product.move_out_of_stock_on","product.move_out_of_stock_on"):
                   sOutput+=","
                   continue
                elif  srcRow[fldlist[srcDesc[0]]] in ("(null)","NULL","null") and tgtRow[fldlist[srcDesc[0]]]==None:
                    sOutput+=","
                    continue
                if (srcRow[fldlist[srcDesc[0]]]==None and tgtRow[fldlist[srcDesc[0]]]!=None) or (srcRow[fldlist[srcDesc[0]]]!=None and tgtRow[fldlist[srcDesc[0]]]==None):
                    sOutput+=str(srcRow[fldlist[srcDesc[0]]])+" @ " + str(tgtRow[fldlist[srcDesc[0]]])
                    diff="Y"
                    continue
                if srcRow[fldlist[srcDesc[0]]]!=str(tgtRow[fldlist[srcDesc[0]]]):
                    #print(srcDesc[0],srcDesc[1],tgtDesc[0],tgtDesc[1])
                    if isinstance(tgtRow[fldlist[srcDesc[0]]],bool):
                        if (srcRow[fldlist[srcDesc[0]]]=="NULL" and tgtRow[fldlist[srcDesc[0]]]) or (srcRow[fldlist[srcDesc[0]]]=="1" and not tgtRow[fldlist[srcDesc[0]]]):
                            sOutput+=str(srcRow[fldlist[srcDesc[0]]])+" @ " + str(tgtRow[fldlist[srcDesc[0]]])
                            diff="Y"
                            sOutput+=","
                            flderrcnt[i]=flderrcnt[i]+1
                        else:
                            sOutput+=","                
                    elif isinstance(tgtRow[fldlist[srcDesc[0]]],datetime.datetime):
                        if datetime.datetime.strptime(srcRow[fldlist[srcDesc[0]]], '%Y-%m-%d')!=tgtRow[fldlist[srcDesc[0]]]:
                            sOutput+=str(srcRow[fldlist[srcDesc[0]]])+" @@ " + str(tgtRow[fldlist[srcDesc[0]]])
                            sOutput+=","                
                            flderrcnt[i]=flderrcnt[i]+1
                            diff="Y"
                        else:
                            sOutput+=","
                    elif isinstance(tgtRow[fldlist[srcDesc[0]]],datetime.date):
                        if  srcRow[fldlist[srcDesc[0]]][0:10]!=str(tgtRow[fldlist[srcDesc[0]]]):
                            sOutput+=str(srcRow[fldlist[srcDesc[0]]])+" @@@ " + str(tgtRow[fldlist[srcDesc[0]]])
                            flderrcnt[i]=flderrcnt[i]+1
                            sOutput+=","                
                            diff="Y"
                        else:
                            sOutput+=","
                    elif isinstance(tgtRow[fldlist[srcDesc[0]]],int):
                        #print(tgtRow[fldlist[srcDesc[0]]],srcDesc[0])
                        if int(srcRow[fldlist[srcDesc[0]]])!=int(tgtRow[fldlist[srcDesc[0]]]):
                            sOutput+=str(srcRow[fldlist[srcDesc[0]]])+" @@@@ " + str(tgtRow[fldlist[srcDesc[0]]])
                            sOutput+=","                
                            flderrcnt[i]=flderrcnt[i]+1
                            diff="Y"
                        else:
                            sOutput+=","
                    elif isinstance(tgtRow[fldlist[srcDesc[0]]],decimal.Decimal):
                        if float(srcRow[fldlist[srcDesc[0]]])!=float(tgtRow[fldlist[srcDesc[0]]]):
                            sOutput+=str(srcRow[fldlist[srcDesc[0]]])+" @@@@ " + str(tgtRow[fldlist[srcDesc[0]]])
                            flderrcnt[i]=flderrcnt[i]+1
                            sOutput+=","                
                            diff="Y"
                        else:
                            sOutput+=","
                    elif isinstance(tgtRow[fldlist[srcDesc[0]]],str):
                        if not ((srcRow[fldlist[srcDesc[0]]]=="(null)" and tgtRow[fldlist[srcDesc[0]]]==None) or (srcRow[fldlist[srcDesc[0]]]=="NULL" and tgtRow[fldlist[srcDesc[0]]]==None)):
                            sOutput+=str(srcRow[fldlist[srcDesc[0]]])+" @@@@@ " + str(tgtRow[fldlist[srcDesc[0]]])
                            flderrcnt[i]=flderrcnt[i]+1
                            sOutput+=","                
                            diff="Y"
                        else:
                            sOutput+=","
                    else:
                        sOutput+=","                 
                else:
                    #print(str(srcRow[fldlist[srcDesc[0]]])+" = " + str(tgtRow[fldlist[srcDesc[0]]]))
                    sOutput+=","
            if diff=="Y":
                #print(srcRow[0], srcRow[1],srcRow)
                sOutput=str(srcRow[0]) + "," + sOutput[1:]
                rowMismatch+=1
                dFile.write(sOutput + "\n")
                if rowMismatch>2000:
                    print("[E] ***** Terminating Validation as there are than 2000+ Erros....")
                    iLoop=1000
                    break
            srcRow=scur.fetchone()
            tgtRow=tcur.fetchone()
        elif srcRow[0] >tgtRow[0]:
                #print("In >")
                sOutput=str(tgtRow[0]) + " not found in Source"
                notInSrc+=1
                #print(sOutput)
                dFile.write(sOutput + "\n")
                tgtRow=tcur.fetchone()
        else:
                #print("In <")
                sOutput=str(srcRow[0]) + " not found in Target"
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
for i in range(0,len(flderrcnt)):
    sOutput+=str(flderrcnt[i])+","

dFile.write("\n"+hdr+"\n")
dFile.write(sOutput+"\n")



logFile.write("[I] {} Product Validation completed\n".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))
dFile.flush()
logFile.flush()
dFile.close
logFile.close
logFile=None
dFile=None
print("Validation Ended")

