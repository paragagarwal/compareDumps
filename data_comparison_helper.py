#!/usr/bin/env python
import sys
import os
import glob
import getopt
import ast

""" Class contains logic for data consistency checks 
    1) Comparison of Data in {Key, Exp, Flag, CAS, Rev id, Value} format
       We try to answer the following:
       - if there is a lag between source and destinaiton result set or vice-versa
       - for similar keys, if there is a difference in Exp, Flag, Rev id, CAS
       Usage: Tool output like CB Transfer
    2) Comparison of Data in Jason format (used during replica comparisons)
       {row_counts: xxx, rows {id: <same as key> , key: <same as key>, value: <rev meta data>}}
       We try to answer the following: 
       - if replica is lagging between active vs replica
       - if the active and replica have difference in rev meta data
       Usage: View output in Jason format
"""
class DataComparator(object):
 
    """ Compare View Output in Jason format between Source and Target Directories"""
    @staticmethod
    def compareJasonFormatInfo(srcDir=".",tgtDir=".",srcCnt=1,tgtCnt=1):
        srcFiles=glob.glob(srcDir+"/*")
        tgtFiles=glob.glob(tgtDir+"/*")
        totalSRC={}
        totalCountSRC={}
        totalTGT={}
        totalCountTGT={}
        for file in srcFiles:
            print "Analyzing Src file ::"+file
            count,info=DataComparator.getValueFromJasonResult(file)
            totalSRC.update(info)
            totalCountSRC.update(count)
        for file in tgtFiles:
            print "Analyzing Tgt file ::"+file
            count,info=DataComparator.getValueFromJasonResult(file)
            totalTGT.update(info)
            totalCountTGT.update(count)
        srcMinusTgt,tgtMinusSrc,sameKeyValueDiff=DataComparator.differenceInChangedKeysForJasonResult(totalSRC,totalTGT)
        srcCountResult=DataComparator.compareCountOfReplicas(totalCountSRC,srcCnt)
        tgtCountResult=DataComparator.compareCountOfReplicas(totalCountTGT,tgtCnt)
        return srcCountResult,tgtCountResult,srcMinusTgt,tgtMinusSrc,sameKeyValueDiff

    """ Method to check if count of replicas are consistent """
    @staticmethod
    def compareCountOfReplicas(src={},count=1):
        result={}
        for o in src.keys():
            if src[o] != count:
                result[o]=src[o]
        return result

    """ Compare CSV output between Source and Target Directories
        Assumption:: Output is in CSV format {Key, Exp, Flag, CAS, Rev Id, Value}
    """
    @staticmethod
    def compareDataInfoInCSVFormat(srcDir=".",tgtDir="."):
        srcFiles=glob.glob(srcDir+"/*")
        tgtFiles=glob.glob(tgtDir+"/*")
        totalSRC={}
        totalTGT={}
        print "Analyzing Source Directory"
        for file in srcFiles:
            print "Analyzing file ::"+file
            info=DataComparator.getValueFromCSV(file)
            print "Record(s) Read ::",len(info)
            totalSRC.update(info)
        print "Total Source Records ::",len(totalSRC)
        print "Analyzing Target Directory"
        for file in tgtFiles:
             print "Analyzing file ::"+file
             info=DataComparator.getValueFromCSV(file)
             print "Record(s) Read ::",len(info)
             totalTGT.update(info) 
        print "Total Target Records ::",len(totalTGT)
        srcMinusTgt,tgtMinusSrc,sameKeyValueDiff=DataComparator.differenceInChangedKeys(totalSRC,totalTGT)
        return srcMinusTgt,tgtMinusSrc,sameKeyValueDiff

   
    """ The method parses through a all jason result set
        dump given format and builds hash map 
        for key of documents and value as rev Ids
    """ 
    @staticmethod
    def getValueFromJasonResult(filePath):
        with open(filePath, 'r') as f:
            s = f.read()
        dict=ast.literal_eval(s)
        info={}
        count={}
        for r in dict['rows']:
            if r['key'] in info.keys():
                count[r['key']]=count[r['key']]+1
            else:
                info[r['key']]=r['value']
                count[r['key']]=1
        return count,info

    """ Extract information from file
        Data Format assumption {key, Exp, Flag, CAS, Rev id, Value}
    """   
    @staticmethod
    def getValueFromCSV(filePath):
        info={}
        try:
            for line in open(filePath):
                values=line.split(",")
                if len(values) >= 6:
                    info[values[0]]=[values[1],values[2],values[3],values[4],values[5:]]
        except Exception, err:
            sys.stderr.write('ERROR: %s\n' % str(err))
        return info
     
    """ Find the difference between two key,rev id pairs maps 
        1) Src Key Map - Tgt Key Map
        2) Tgt Key Map - Src Key Map
        3) Change in Values for Common Keys
    """
    @staticmethod
    def differenceInChangedKeys(dict1,dict2):
        s1=set(dict1.keys())
        s2=set(dict2.keys())
        diff1={}
        diff2={}
        diff3={}
        for o in s1.difference(s2):
            diff1[o]=dict1[o]
        for o in s2.difference(s1):
            diff2[o]=dict2[o]
        for o in s1.intersection(s2):
            flag,message=DataComparator.differenceInValuesInCSVFormat(dict1[o],dict2[o])
            if flag:
                diff3[o]=message
        return diff1,diff2,diff3
  
    @staticmethod
    def differenceInChangedKeysForJasonResult(dict1,dict2):
        s1=set(dict1.keys())
        s2=set(dict2.keys())
        diff1={}
        diff2={}
        diff3={}
        for o in s1.difference(s2):
            diff1[o]=dict1[o]
        for o in s2.difference(s1):
            diff2[o]=dict2[o]
        for o in s1.intersection(s2):
            if dict1[o] != dict2[o]:
                diff3[o]=[dict1[o],dict2[o]]
        return diff1,diff2,diff3

    """ Find differences in values for {Flag, Exp, CAS, Rev id, Value} result set """
    @staticmethod
    def differenceInValuesInCSVFormat(val1,val2):
        message={}
        flag=False
        if(val1[0:1] != val2[0:1]):
            flag=True
            message['flag']=val1[0:1],val2[0:1]
        if(val1[1:2] != val2[1:2]):
            flag=True
            message['Exp']=val1[1:2],val2[1:2]
        if(val1[2:3] != val2[2:3]):
            flag=True
            message['CAS']=val1[2:3],val2[2:3]
        if(val1[3:4] != val2[3:4]):
            flag=True
            message['Rev']=val1[3:4],val2[3:4]
        if(val1[4:5] != val2[4:5]):
            flag=True
            message['Value']=val1[4:5],val2[4:5]
        return flag,message

    """ Print the analysis results for CSV format """
    @staticmethod
    def printResultOfCSVFormatAnalysis(diff1,diff2,diff3):
        print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
        print "Analysis of Source and Target Directory Comparison Results"
        print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
        print """1) Difference between Source and Target Keys """
        for o in diff1.keys():
            print "\n Key :: "+o+"::",DataComparator.printAllValues(diff1[o])
        print "\n number of such cases ::",len(diff1)
        print "----------------------------------------------------------"
        print """2) Difference between Target and Source Keys """
        for o in diff2.keys():
            print "\n Key :: "+o+"::",DataComparator.printAllValues(diff2[o])
        print "\n number of such cases ::",len(diff2)
        print "----------------------------------------------------------"
        print """3) Change Value Analysis for same Keys """
        for o in diff3.keys():
            print "\n For Key :: "+o
            for k in diff3[o]:
                print "\n",k,"::",diff3[o][k][0]," changes to ",diff3[o][k][1]
            print "\n ======================================================"
        print "number of such cases ::",len(diff3)

    """ Print result of Jason Format Analysis """
    @staticmethod
    def printResultOfJasonFormatAnalysis(diff1,diff2,diff3,srcCount,tgtCount):
        print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
        print "Analysis of Source and Target Directory Comparison Results"
        print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
        print """1) Difference between Source and Target Keys """
        for o in diff1.keys():
            print "\n     Key :: "+o+" with Information :: ",diff1[o]
        print "number of such cases ::",len(diff1)
        print "\n ----------------------------------------------------------"
        print """2) Difference between Target and Source Keys """
        for o in diff2.keys():
            print "\n     Key :: "+o+" with Information :: ",diff2[o]
        print "number of such cases ::",len(diff2)
        print "\n ----------------------------------------------------------"
        print """3) Change Value Analysis for same Keys """
        for o in diff3.keys():
            print "\n For Key :: "+o
            for key in diff3[o].keys():
                print "\n"+key+"::",diff3[o][key]
        print "number of such cases ::",len(diff2)
        print """4) Source Replica Count Issues """
        for o in srcCount.keys():
            print "\n",[o,srcCount[o]]
        print "number of such cases ::",len(srcCount)
        print """5) Target Replica Count Issues """
        for o in tgtCount.keys():
            print "\n",[o,tgtCount[o]]
        print "number of such cases ::",len(tgtCount)
    
    """ Print in format for {Exp, Flag, CAS, Rev Id, Value} """
    @staticmethod
    def printAllValues(data=[]):
        str={}
        str['Exp']=data[0]
        str['Flag']=data[1]
        str['CAS']=data[2]
        str['Rev Id']=data[3]
        str['Value']=data[4]
        return str
