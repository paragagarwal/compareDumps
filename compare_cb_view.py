#!/usr/bin/env python
import sys
import os
import glob
import getopt
import ast

""" The usage method"""
def usage(error=None):
    print """\
    This tool compares the output dumps of data views or cbTransfer
    to check if there is data inconsistency between dump

    cbTransfer: We assume data format is as follows: {Key, Exp, Flag. CAS, Rev Id, Value}
    Views: We assume data format is as follows: Key, Value, rev Id 

    Syntax: compare_cb_view.py -s sourceDir -t targetDir -mode modeType [options]

    [Required Parameters]
    -s      :: Source Directory for comparison
    -t      :: Target Directory for comparison
    -mode   :: view :: used to compare all view output for clusters
               cbt :: used to compare all cbtansfer output for clusters
    [Optional Parameters]
    -h      :: Help, will list the usage
    -?      :: Help, will list the usage
    --help  :: Help, will list the usage
    -c      :: Expected Replica count. Parameter used during view mode
    Examples:
        compare_cb_docs.py -s ./source -t ./target -mode view -c 1
        compare_cb_docs.py -s ./source -t ./target -mode cbt
        compare_cb_docs.py -?
        compare_cb_docs.py --help
        compare_cb_docs.py -h
    """

""" Class that contains methods to compare data information downloaded
    from clusters using all docs command or cbTransfer"""
class DataComparator(object):
 
    """ Compare Views between Source and Target Directories"""
    @staticmethod
    def compareViewInfo(srcDir=".",tgtDir=".",srcCnt=1,tgtCnt=1):
        srcFiles=glob.glob(srcDir+"/*")
        tgtFiles=glob.glob(tgtDir+"/*")
        totalSRC={}
        totalCountSRC={}
        totalTGT={}
        totalCountTGT={}
        for file in srcFiles:
            print "Analyzing Src file ::"+file
            count,info=DataComparator.getValueFromViewResult(file)
            totalSRC.update(info)
            totalCountSRC.update(count)
        for file in tgtFiles:
            print "Analyzing Tgt file ::"+file
            count,info=DataComparator.getValueFromViewResult(file)
            totalTGT.update(info)
            totalCountTGT.update(count)
        srcMinusTgt,tgtMinusSrc,sameKeyValueDiff=DataComparator.differenceInChangedKeysForViewResult(totalSRC,totalTGT)
        srcCountResult=DataComparator.compareCountOfReplicas(totalCountSRC,srcCnt)
        tgtCountResult=DataComparator.compareCountOfReplicas(totalCountTGT,tgtCnt)
        return srcCountResult,tgtCountResult,srcMinusTgt,tgtMinusSrc,sameKeyValueDiff

    """ Method to check if count if replicas are consistent """
    @staticmethod
    def compareCountOfReplicas(src={},count=1):
        result={}
        for o in src.keys():
            if src[o] != count:
                result[o]=src[o]
        return result

    """ Compare CBT output between Source and Target Directories"""
    @staticmethod
    def compareCBT(srcDir=".",tgtDir="."):
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

   
    """ The method parses through a all ddocs
        dump given format and builds hash map 
        for key of documents and value as rev Ids
    """ 
    @staticmethod
    def getValueFromViewResult(filePath):
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

    """ The method parses through a cbTransfer
        dump given csv and builds hash map 
        for key of documents and value as rev Ids
    """   
    @staticmethod
    def getValueFromCSV(filePath):
        info={}
        try:
            for line in open(filePath):
                str1=line+""
                values=str1.split(",")
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
            flag,message=DataComparator.differenceInValues(dict1[o],dict2[o])
            if flag:
                diff3[o]=message
        return diff1,diff2,diff3
  
    @staticmethod
    def differenceInChangedKeysForViewResult(dict1,dict2):
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

    """ Find differences in values for CBT result set """
    @staticmethod
    def differenceInValues(val1,val2):
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

    """ Print the analysis results """
    @staticmethod
    def printResult(diff1,diff2,diff3):
        print "Analysis of Source and Target Directory Comparison Results"
        print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
        print """1) Difference between Source and Target Keys """
        for o in diff1.keys():
            print "\n     Key :: "+o+"::",diff1[o]
        print "number of such cases ::",len(diff1)
        print "----------------------------------------------------------"
        print """2) Difference between Target and Source Keys """
        for o in diff2.keys():
            print "\n     Key :: "+o+"::",diff2[o]
        print "number of such cases ::",len(diff2)
        print "----------------------------------------------------------"
        print """3) Change Value Analysis for same Keys """
        for o in diff3.keys():
            print "\n For Key :: "+o
            print "\n"+key+"::",diff3[o][key]
        print "number of such cases ::",len(diff3)

    """ Print result of View Analysis """
    @staticmethod
    def printResultOfViewAnalysis(diff1,diff2,diff3,srcCount,tgtCount):
        print "Analysis of Source and Target Directory Comparison Results"
        print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
        print """1) Difference between Source and Target Keys """
        for o in diff1.keys():
            print "\n     Key :: "+o+"::",diff1[o]
        print "number of such cases ::",len(diff1)
        print "----------------------------------------------------------"
        print """2) Difference between Target and Source Keys """
        for o in diff2.keys():
            print "\n     Key :: "+o+"::",diff2[o]
        print "number of such cases ::",len(diff2)
        print "----------------------------------------------------------"
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

def main():
    src="NONE"
    tgt="NONE"
    mode="NONE"
    replicaSrc=1
    replicaTgt=1
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], 'c:s:t:m:h', ["mode","mode=","src=","tgt="])
        for o, a in opts:
            if o == "-h" or o ==  "--help":
                usage()
            if o in ("-s","--src"):
                src=a
            elif o in ("-t","--tgt"):
                tgt=a
            elif o in ("-m","--mode"):
                mode=a
            elif o in ("-c","--count"):
                replicaTgt=int(a)
        if src == "NONE" or tgt == "NONE" or mode == "NONE":
            print "ERROR :: Missing Required Parameters"
            usage()
            sys.exit()
        if mode == "cbt":
            s1,s2,s3 = DataComparator.compareCBT(src,tgt)
            DataComparator.printResult(s1,s2,s3)
        elif mode == "view":
            c1,c2,s1,s2,s3 = DataComparator.compareViewInfo(src,tgt,1,replicaTgt)
            DataComparator.printResultOfViewAnalysis(s1,s2,s3,c1,c2) 
    except error:
        usage()
    except getopt.GetoptError, error:
        usage("ERROR: " + str(error)) 

if __name__ == "__main__":
    main()
