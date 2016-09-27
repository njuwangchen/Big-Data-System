#!/usr/bin/env python2
import sys
import os
import glob
import shutil
import re

vmNum = 4
queryId = (12,21,50,71,85)

# output
readSec = []
writeSec = []
Rx = []
Tx = []


def getDiskData(MRvsTez):
    if (MRvsTez == 1):
        filePath = "disk/"
    else:
        filePath = "disk_tez/"
    for i in range(len(queryId)):
        # common directory
        comDir = "outputv4/" + str(queryId[i]) + "/"
        readSum = 0
        writeSum = 0
        for j in range(vmNum):
            
            filename = comDir + filePath + "after_vm" + str(j + 1)
            fileCont = open(filename)
            after1, after2 = extractDisk(fileCont)
            fileCont.close()

            filename = comDir + filePath + "before_vm" + str(j + 1)
            fileCont = open(filename)
            before1, before2 = extractDisk(fileCont)
            fileCont.close()
 
            readSum += (after1 - before1)
            writeSum += (after2 - before2)
            
            # correctness check
            # print queryId[i], j, (after1 - before1), (after2 - before2)
        # print readSum, writeSum
        readSec.append(readSum)
        writeSec.append(writeSum)

def getNetData(MRvsTez):
    if (MRvsTez == 1):
        filePath = "net/"
    else:
        filePath = "net_tez/"

    for i in range(len(queryId)):
        # common directory
        comDir = "outputv4/" + str(queryId[i]) + "/"
        readSum = 0
        writeSum = 0
        for j in range(vmNum):

            filename = comDir + filePath + "after_vm" + str(j + 1)
            fileCont = open(filename)
            after1, after2 = extractNet(fileCont)
            fileCont.close()

            filename = comDir + filePath + "before_vm" + str(j + 1)
            fileCont = open(filename)
            before1, before2 = extractNet(fileCont)
            fileCont.close()

            readSum += (after1 - before1)
            writeSum += (after2 - before2)
        Rx.append(readSum)
        Tx.append(writeSum)
           

def extractDisk(fileCont):
    readTotal = 0
    writeTotal = 0
    for line in fileCont:
        if re.match("(.*)sd[abc]1(.*)",line):
            #print line
            tmpLine = line.split()
            readTotal += int(tmpLine[5])
            writeTotal += int(tmpLine[9])
            #print tmpLine[5], tmpLine[9], readTotal, writeTotal
    return readTotal, writeTotal
            
def extractNet(fileCont):
    netRxTotal = 0
    netTxTotal = 0
    for line in fileCont:
        if re.match("(.*)eth0(.*)",line):
            #print line, type(line)
            tmpLine = line.split()
            #print tmpLine
            netRxTotal += int(tmpLine[1])
            netTxTotal += int(tmpLine[9])
    return netRxTotal, netTxTotal 


def writeResult(filename, data1, data2):
    outputFile = open(filename, 'w')
    for i in range(len(queryId)):
        #print queryId[i]
        outputFile.write(str(queryId[i]) + " " + str(data1[i]) + " " + str(data2[i]) + "\n")
    outputFile.close()

def emptyRes():
    readSec = []
    writeSec =[]
    Rx = []
    Tx = []

def main():
        
    getNetData(1)
    #print Rx, Tx

    getDiskData(1)
    #print readSec, writeSec
    writeResult("MR_disk", readSec, writeSec)
    writeResult("MR_net", Rx, Tx)
    emptyRes()
    
    getNetData(0)
    getDiskData(0)
    writeResult("Tez_disk", readSec, writeSec)
    writeResult("Tez_net", Rx, Tx)
    

if __name__ == "__main__":
    main()
