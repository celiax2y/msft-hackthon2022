# Databricks notebook source
from azure.cognitiveservices.vision.computervision import ComputerVisionClient
from azure.cognitiveservices.vision.computervision.models import OperationStatusCodes
from azure.cognitiveservices.vision.computervision.models import VisualFeatureTypes
from msrest.authentication import CognitiveServicesCredentials

from array import array
import os
from PIL import Image
import sys
import time
import io
import math

import pandas as pd

'''
Authenticate
Authenticates your credentials and creates a client.
'''
subscription_key = "xxxxxxxx"  # replace with your resource subscription key
endpoint = "https://xxxx.com/" # replace with the endpoint url

computervision_client = ComputerVisionClient(endpoint, CognitiveServicesCredentials(subscription_key))
'''
END - Authenticate
'''

'''
OCR: Read File using the Read API, extract text - remote
This example will extract text in an image, then print results, line by line.
This API call can also extract handwriting style text (not shown).
'''
print("===== Read File - remote =====")
# Get an image with text

def ocr(filePath, file):
  currentPath = filePath + file
  df = spark.read.format("binaryFile").load(currentPath)
  file_stream = io.BytesIO(df.select('content').rdd.map(lambda x: x[0]).collect()[0])
  read_response = computervision_client.read_in_stream(file_stream,raw=True)
  # Get the operation location (URL with an ID at the end) from the response
  read_operation_location = read_response.headers["Operation-Location"]
  # Grab the ID from the URL
  operation_id = read_operation_location.split("/")[-1]

  # Call the "GET" API and wait for it to retrieve the results 
  while True:
    read_result = computervision_client.get_read_result(operation_id)
    if read_result.status not in ['notStarted', 'running']:
      break
    time.sleep(1)

  # Print the detected text, line by line
  if read_result.status == OperationStatusCodes.succeeded:
    for text_result in read_result.analyze_result.read_results:
      for line in text_result.lines:
        print(line.text)
        print(line.bounding_box)
  return text_result


def stringConcatination(text_result):
  d = {}
  substrs = []
  for i in range(len(text_result.lines)):
    key = text_result.lines[i].text
    value = text_result.lines[i].bounding_box
    d[key] = value
  #print(d)
  dsorted = sorted(d.items(), key=lambda x:x[1][0])
  sortdict = dict(dsorted)
  #print(sortdict)

  coorV = []
  for x in sortdict:
    leftbottom =  sortdict[x][0]
    coorV.append(leftbottom)
  bitflg = [0]* len(coorV)
  for i in range(len(coorV) - 1):
    diffV = abs(coorV[i] - coorV[i + 1])
    #print('diffV is ', diffV)
    if (diffV <= 60.0):
      bitflg[i] = 1
  cnt = 0
  concatenatedStr = ''
  for item in sortdict:
    if (bitflg[cnt]):
      concatenatedStr += item     
    else:
      concatenatedStr += item
      concatenatedStr += ";"
    cnt += 1
  concatenatedStr = concatenatedStr[:-1]
  print(concatenatedStr)
  return concatenatedStr

def BOMcheck(inputbom):
  if(len(inputbom) != 8):
    print("BOM format wrong: wrong length.\n")
    return False
  if not (inputbom[-1].isupper()):
    #print(inputbom[-1])
    print("BOM format wrong: last spot not uppercase.\n") 
    return False
  for i in range(len(inputbom) - 1):
    if not (inputbom[i].isdigit()):
      print("BOM format wrong: at least one non-digt in the first 7 spots.\n")
      return False
  print("BOM check pass")
  return True

def MSFcheck(input):
  item = input.split('-')[1].lstrip()
  if(len(item) != 6): 
    print("MSF format wrong: wrong length.\n")
  for i in item:
    if not (i.isdigit()):
      print("MSF format wrong: at least one non-digit.\n")
      return False
  print("MSF check pass.\n")
  return True

def MSAssetcheck(input):
  input = input.lstrip().rstrip()
  print(input)
  if (len(input) != 8):
    print("MS-Asset format wrong: wrong length.\n")
    return 
  for i in range(len(input)):
    if not(input[i].isdigit()):
      print("MSAsset format wrong: at least one non-digt.\n")
  print("MSAsset check pass.\n")
  return True

def COOcheck(input):
  if not ("Assembled in" in input): 
    print("COO format wrong: \"Assembled in\" not included in the label.\n")
    return False  
  print("COO check pass.\n")
  return True


def SNcheck(input):
  # not complete check yet
  input = input.split(":")[1].lstrip().rstrip()
  print(input)
  if input[0] != "R":
    print("SN didn't start with R")
    return False
  if len(input)!= 16:
    print("SN length wrong")
  return True
def MSPNcheck(input):
  input = input.split(":")[1].lstrip().rstrip()
  if input[0] != "M":
    errorMsg = "MSPN didn't start with M"
    return [errorMsg, False]
  substrs = input.split()
  if len(substrs) != 3:
    errorMsg = "MSPN format wrong"
    return [errorMsg, False]
  # more logic added for MSPN details
  if len(substrs[2])!= 1:
    errorMsg = "MSPN format wrong"
    return [errorMsg, False]
  return ["Pass", True]

# three test cases 

#fileList = ["1_left.jpg", "1_right.jpg", "1_all.png"]
#fileList = ["1_left.jpg"]
#barcodeFile = "barcode1.csv"
#filePath = "xxxxx"

fileList = ["1_left.JPG", "2_right.png", "3_all.png"]
#fileList = ["2_right.png"]
barcodeFile = "barcode2.csv"
filePath = "xxxxx"   # mounted ADSL Gen2 storage account on databricks filesystem

#fileList = ["3_left.jpg", "3_right.jpg", "3_all.jpg"]
#barcodeFile = "barcode3.csv"
#filePath = "xxxx"

output = {'Requirement':["L11 BOM Tag is Present",
                      "L11 COO Label is Present",
                      "L11 MSF Label is Present",
          "L11 Asset Tag is Present",
          "L11 MSPN SN Label is Present",
          "L11 BOM Tag is in the Correct Format",
          "L11 COO Label is the Correct Format",
          "L11 MSF Label is in the Correct Format",
          "L11 Asset Tag is in the Correct Format",
          "L11 MSPN SN Label is in the Correct Format",
          "L11 BOM Tag is in Correct Location",
          "L11 COO Label is in Correct Location",
          "L11 MSF Label is in the Correct Location",
          "L11 Asset Tag is in the Correct Location",
          "L11 MSPN SN Label is in the Correct Location",
          "L11 BOM Barcode Matches Text",
          "L11 MSF Label Barcode Matches Text",
          "L11 Asset Tag Barcode Matches Text",
          "L11 MSPN Barcode Matches Text",
          "L11 SN Barcode Matches Text",
          "L11 BOM Tag is in Good Condition",
          "L11 COO Label is in Good Condition",
          "L11 MSF Label is in Good Condition",
          "L11 Asset Tag is in Good Condition",
          "L11 MSPN SN Label is in Good Condition"],
        'Pass/Fail/Error': [""] * 25,
        'Notes':[""] * 25}

pdf = pd.DataFrame(output)


pdfbarcode = pd.DataFrame()
if barcodeFile != "":
  barcodes = spark.read.csv(filePath + barcodeFile)
  pdfbarcode = barcodes.toPandas()

for file in fileList:
  if (file.find("left")>= 0):
    #offset is 0 and 5
    print("left check")
    left_textoutput = ocr(filePath, file)
    concatenatedStrLeft = stringConcatination(left_textoutput)
    # format check for left group
    substrs = []
    substrs = concatenatedStrLeft.split(";")
    cnt = 0
    if concatenatedStrLeft.startswith("BOM"):
      #print(BOMcheck(substrs[1]))
      if(concatenatedStrLeft.find("BOM") >= 0):
        pdf.at[0 + cnt, 'Pass/Fail/Error'] = "Pass"
        BOMflag = 1
      else:
        pdf.at[0 + cnt, 'Pass/Fail/Error'] = "Fail"
        BOMflag = 0
      cnt += 1
        
      if(concatenatedStrLeft.find("Assemble") >= 0):
        pdf.at[0 + cnt, 'Pass/Fail/Error'] = "Pass"
        COOflag = 1
      else:
        pdf.at[0 + cnt, 'Pass/Fail/Error'] = "Fail"
        COOflag = 0
      cnt += 1
      if(concatenatedStrLeft.find("MSF-") >= 0):
        pdf.at[0 + cnt, 'Pass/Fail/Error'] = "Pass"
        MSFflag = 1
      else:
        pdf.at[0 + cnt, 'Pass/Fail/Error'] = "Fail"
        MSFflag = 0
        cnt += 1
      cnt = 0
      barcodecnt = 0
      if (BOMcheck(substrs[1])):        
        pdf.at[5 + cnt, 'Pass/Fail/Error'] = "Pass"
      else:
        pdf.at[5 + cnt, 'Pass/Fail/Error'] = "Fail"
      cnt += 1
      restlen = len(substrs) - 2        
      if restlen == 0: 
          print("Job done.\n")
      elif restlen == 1: 
          restStr = ";".join(substrs[2:])
          print(restStr)
          if COOflag:
            if COOcheck(restStr):
              pdf.at[5 + cnt, 'Pass/Fail/Error'] = "Pass"
            cnt += 1
          if ~MSFflag:
            pdf.at[5 + cnt, 'Pass/Fail/Error'] = "Fail"
      elif restlen == 2:
          if(COOcheck(substrs[2]) ):
            pdf.at[5 + cnt, 'Pass/Fail/Error'] = "Pass"  
          else:
            pdf.at[5 + cnt, 'Pass/Fail/Error'] = "Fail"  
          cnt += 1
          if MSFflag:                        
            if MSFcheck(substrs[-1]):
              pdf.at[5 + cnt, 'Pass/Fail/Error'] = "Pass"  
            else:
              pdf.at[5 + cnt, 'Pass/Fail/Error'] = "Fail"  
          else:
            pdf.at[5 + cnt, 'Pass/Fail/Error'] = "Fail"
      if pdfbarcode.loc[0, "_c1"] == substrs[1].lstrip().rstrip():
          pdf.at[15+barcodecnt, 'Pass/Fail/Error'] = "Pass"
      else:
          pdf.at[15+barcodecnt, 'Pass/Fail/Error'] = "Fail"
      barcodecnt += 1        
      if pdfbarcode.loc[1, "_c1"] == substrs[-1].lstrip().rstrip():
          pdf.at[15+barcodecnt, 'Pass/Fail/Error'] = "Pass"
      elif pdfbarcode.loc[1, "_c1"] == None:
          pdf.at[15+barcodecnt, 'Notes'] = "Barcode not detected"  
          pdf.at[15+barcodecnt, 'Pass/Fail/Error'] = "Fail"  
      else:
          pdf.at[15+barcodecnt, 'Pass/Fail/Error'] = "Fail"   
  elif (file.find("right") >= 0) :
    #offset 3, 8
    barcodecnt = 0
    print("right check")
    right_textoutput = ocr(filePath, file)
    concatenatedStrRight = stringConcatination(right_textoutput)
    # format check for right group
    substrs = []
    substrs = concatenatedStrRight.split(";")
    cnt = 0
    if concatenatedStrRight.startswith("MS"):
      if(concatenatedStrRight.find("MS") >= 0 and concatenatedStrRight.find("Asset") >= 0):
        if concatenatedStrRight.find("MS") + 2 == concatenatedStrRight.find("Asset"):
          concatenatedStrRight = concatenatedStrRight.replace("MS;Asset", "MSAsset")
        pdf.at[3 + cnt, 'Pass/Fail/Error'] = "Pass"
        cnt += 1
      if (concatenatedStrRight.find("SN") >= 0 and concatenatedStrRight.find("MSPN") >= 0):
        pdf.at[3 + cnt, 'Pass/Fail/Error'] = "Pass"
      cnt = 0
      if (MSAssetcheck(substrs[1])):
        pdf.at[8 + cnt, 'Pass/Fail/Error'] = "Pass"
      if pdfbarcode.loc[2, "_c1"] == substrs[1].lstrip().rstrip():
          pdf.at[17 + barcodecnt, 'Pass/Fail/Error'] = "Pass"
      elif pdfbarcode.loc[2, "_c1"] ==None:
          pdf.at[17 + barcodecnt, 'Notes'] = "Barcode not detected"
          pdf.at[17 + barcodecnt, 'Pass/Fail/Error'] = "Fail"
      else:
          pdf.at[17 + barcodecnt, 'Pass/Fail/Error'] = "Fail"
      barcodecnt += 1
      cnt += 1
      if (SNcheck(substrs[2]) and MSPNcheck(substrs[3])[1]):
        pdf.at[8 + cnt,'Pass/Fail/Error'] = "Pass"
      
      else:
        errormsg = MSPNcheck(substrs[3])[0]
        pdf.at[8 + cnt,'Pass/Fail/Error'] = "Fail"
        pdf.at[8 + cnt,'Notes'] = errormsg
      if pdfbarcode.loc[3, "_c1"] == substrs[3].split(":")[1].split()[0].lstrip().rstrip():
          pdf.at[17 + barcodecnt, 'Pass/Fail/Error'] = "Pass"
      elif pdfbarcode.loc[3, "_c1"] ==None:
          pdf.at[17 + barcodecnt, 'Notes'] = "Barcode not detected"
          pdf.at[17 + barcodecnt, 'Pass/Fail/Error'] = "Fail"
      else:
          pdf.at[17 + barcodecnt, 'Pass/Fail/Error'] = "Fail"
      barcodecnt += 1
      if pdfbarcode.loc[4, "_c1"] == substrs[2].split(":")[1].lstrip().rstrip():
          pdf.at[17 + barcodecnt, 'Pass/Fail/Error'] = "Pass"
      elif pdfbarcode.loc[4, "_c1"] ==None:
          pdf.at[17 + barcodecnt, 'Notes'] = "Barcode not detected"
          pdf.at[17 + barcodecnt, 'Pass/Fail/Error'] = "Fail"
      else:
          pdf.at[17 + barcodecnt, 'Pass/Fail/Error'] = "Fail"
        
        
  elif (file.find("all") >= 0) :  #[10-14]
    all_textoutput = ocr(filePath, file)
    concatenatedStrAll = stringConcatination(all_textoutput)
    # location check for all
    seqV = {}
    BOM = concatenatedStrAll.find('BOM')
    if BOM != -1:
      seqV["BOM"] = BOM
    else:
      print("BOM not found.\n")
    COO = concatenatedStrAll.find("Assembled")
    if COO != -1:
      seqV["COO"] = COO
    else:
      print("COO not found.\n")
    MSF = concatenatedStrAll.find("MSF-")
    if MSF != -1:
      seqV["MSF"] = MSF
    else:
      print("MSF not found.\n")
    Asset = concatenatedStrAll.find("Asset")
    if Asset != -1:
      seqV["Asset"] = Asset
    else:
      print("MSAsset not found.\n")

    MSPN = concatenatedStrAll.find("MSPN")
    if MSPN != -1:
      seqV["MSPN"] = MSPN
    else:
      print("MSPN not found.\n")
    seqsorted = sorted(seqV.items(), key=lambda x:x[1])
    sortseqV = dict(seqsorted)
    seqsorted = sorted(seqV.items(), key=lambda x:x[1])
    sortseqV = dict(seqsorted)
    cnt = 0
    xlist = ["BOM", "COO", "MSF", "Asset", "MSPN"]
    for x in xlist:
      if x == "BOM":
        content = "L11 " + x + " Tag is in Good Condition"
      elif x == "COO":
          #"L11 MSPN SN Label is in Good Condition"
        content = "L11 " + x + " Label is in Good Condition"
      elif x == "MSF":
        content = "L11 " + x + " Label is in Good Condition"
      elif x == "Asset":
        content = "L11 " + x + " Tag is in Good Condition"
      elif x == "MSPN":
        content = "L11 " + x + " SN Label is in Good Condition"
      if (x in seqV.keys()) and (seqV[x] == sortseqV[x]):
        pdf.at[10 + cnt, 'Pass/Fail/Error'] = "Pass"
      else:
        pdf.at[10 + cnt, 'Pass/Fail/Error'] = "Fail"
      cnt += 1
      
# convert pandas df to pyspark df
# write pyspark df to ADSL Gen2 storage account
