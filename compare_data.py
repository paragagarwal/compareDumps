#!/usr/bin/env python
import sys
import os
import glob
import getopt
import ast

sys.path.extend(('.', 'lib'))

""" The usage method"""
def usage(error=None):
    print """\
    The tool helps in data consistency analysis. It has two major goals:

    1) Comparison of Data in {Key, Exp, Flag, CAS, Rev id, Value} format for two different output sources
       
       This comparison checks the following:
       - If there is a lag between source and destinaiton result set or vice-versa
       - for similar keys, if there is a difference in Exp, Flag, Rev id, CAS
       
       Used for output for CB transfer which can dump in the given format

        Syntax: compare_data.py -s sourceDir -t targetDir -mode "cbt"
        Example: compare_data.py -s ./source -t ./target -mode cbt
     
    2) Comparison of Data in Jason format (used during replica comparisons) for active and replica data sources
       This comparison checks the following: 
       - If replica is lagging between active vs replica
       - If the active and replica have difference in rev meta data
    
        Used for comparisng consistency in active and replica information
        
        This output dump is from a view created against a bucket. The view is defined as follows:
            function (doc, meta) {
                emit(meta.id, meta.rev, doc.key);
            }

        Jason Format Assumpption {row_counts: xxx, rows {id: <same as key> , key: <same as key>, value: <rev meta data>}}
        Syntax: compare_cb_docs.py -s sourceDir -t targetDir -mode modeType "view" -c <replica count number>
        Example: compare_cb_docs.py -s ./active -t ./replica -mode view -c 1
     
    Parameter Details
    ++++++++++++++++++

    [Required Parameters]
    -s      :: Source Directory for comparison
    -t      :: Target Directory for comparison
    -mode   :: view :: used to compare all view output for clusters
               cbt :: used to compare all cbtansfer output for clusters
    [Optional Parameters]
    -h      :: Help, will list the usage
    -?      :: Help, will list the usage
    --help  :: Help, will list the usage
    -c      :: Expected Replica count. Parameter used only during view mode

    Help Examples
    ++++++++++++++
        compare_data.py -?
        compare_data.py --help
        compare_data.py -h
    """

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
            s1,s2,s3 = data_analysis_helper.DataComparator.compareDataInfoInCSVFormat(src,tgt)
            DataComparator.printResultOfCSVFormatAnalysis(s1,s2,s3)
        elif mode == "view":
            c1,c2,s1,s2,s3 = data_analysis_helper.DataComparator.compareJasonFormatInfo(src,tgt,1,replicaTgt)
            DataComparator.printResultOfJasonFormatAnalysis(s1,s2,s3,c1,c2) 
    except error:
        usage()
    except getopt.GetoptError, error:
        usage("ERROR: " + str(error)) 

if __name__ == "__main__":
    main()
