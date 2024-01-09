import csv
import glob
import os
import re
import sys

#ZITXFFILES = "C:\projects\small_projects\RX-10178\cit.small.in"
#AUTXFFILES = "c:\projects\small_projects\RX-10178"
#PUIAIDMAPPINGFILE = "C:\projects\small_projects\RX-10178\mapping.tsv"
#ZITXFFILES = "smalldb/rx100008/export_file/xf100008d/cit.in"

if len(sys.argv) == 2:
  DATA = sys.argv[1]
else:
  DATA =  "/projects/production/reaxys/dbmake/xf110008d"

# this script based on old export using CI2 to generate data
# script desciption https://elsevier.atlassian.net/browse/RX-10178

#ZITXFFILES = "smalldb/rx100008/fabrication_files/rx100008/xf100008d/cit000.xf"
ZITXFFILES = f"{DATA}/cit*.xf"
#AUTXFFILES = "authcontext"
AUTXFFILES = "/projects/production/auth_back/rawen_out"
PUIAIDMAPPINGFILE = "/projects/production/auth_back/pui2auth-map.tsv"

maxInt = sys.maxsize
print (maxInt)
while True:
  try:
    csv.field_size_limit(maxInt)
    break
  except OverflowError:
    maxInt = int(maxInt/10)
print (maxInt)

# First Citation Pass
citationPUIs = {}
zit_files = glob.glob(os.path.join(DATA, 'cit*.xf'))
for z_f in zit_files:
  with open(z_f, encoding="latin-1") as xffile:
    xfreader = csv.reader(xffile, delimiter="\2")
    for records in xfreader:
      for record in records:
        for fact in record.split('\1'):
          if fact.startswith('R1_'):
            key_data = fact[3:]
          # CIZ PUI
          elif fact.startswith('CIZ'):
            pui_data = fact[3:]
            if key_data and pui_data:
              citationPUIs[pui_data] = int(key_data)
              key_data = ''
              pui_data = ''

print (len(citationPUIs))

# Pass Through Mapping File
# Copy all records having an author ID fromUsedAuthorIDs to a new set of author files.
# Remember the highest author ID () and primary key (AU1).
# set
UsedAuthorIDs = set()
PUIsInBothSources = {}

with open(PUIAIDMAPPINGFILE, encoding="latin-1") as tsvfile:
  tsvreader = csv.reader(tsvfile, delimiter="\t")
  next(tsvreader, None)  # skip the headers
  for line in tsvreader:
    key = line[0]
    if key in citationPUIs:
      UsedAuthorIDs.update(line[1].split(','))
      PUIsInBothSources[key]=line[1].split(',')
    #else:
    #  citationPUIs[key] = line[1]

print(len(UsedAuthorIDs))
#print(UsedAuthorIDs)

#Pass Through Author Context
#Copy all records having an author ID from UsedAuthorIDs to a new set of author files.
reducedAuthorContext=[]
maxAuhorID=0
maxAuhorIDRecord=''

for file_path in glob.glob(os.path.join(AUTXFFILES, "aut*.xf")):
#for file_path in glob.glob(os.path.join(AUTXFFILES, "caut001.xf.small.n")):
  print (file_path)
  with open(file_path, encoding="latin-1") as xffile:
    xfreader = csv.reader(xffile, delimiter="\2")
    for records in xfreader:
      for record in records:
        for fact in record.split('\1'):
          #AU2 is author ID
          if fact.startswith('AU2'):
            authorid = fact[3:]
            if authorid in UsedAuthorIDs:
              reducedAuthorContext.append(record)
              if int(authorid) > maxAuhorID:
                maxAuhorIDRecord = record # to get AUI later
              continue # next record

# Remember the highest author ID () and primary key (AU1).
maxAuthorID=0
maxPrimaryKey=0
print (maxAuhorIDRecord)
for fact in maxAuhorIDRecord.split('\1'):
  if fact.startswith('AU1'):
    maxPrimaryKey = int(fact[3:])
  elif fact.startswith('AU2'):
    maxAuthorID = int (fact[3:])
print (maxAuthorID, maxPrimaryKey)
print (len(reducedAuthorContext))
#print (records)


def write_citation_with_author_id(z_f):
  newRecords =[]
  #regex_pattern = r'(?<!&#x[\da-zA-Z]{4});(?!#)'
  regex_pattern = r'(?<!&#x[\da-fA-F]{4})(?<!&#[\d]{3});(?!#)'
  with open(z_f, encoding="latin-1") as xffile:
    xfreader = csv.reader(xffile, delimiter="\2", skipinitialspace=True)
    for records in xfreader:
      for record in records:
        for fact in record.split('\1'):
          if fact.startswith('R1_'):
            r1_data = fact[3:]
          # CIZ PUI
          elif fact.startswith('CIZ'):
            pui_data = fact[3:]
            if pui_data in PUIsInBothSources:
              # If the the citation has a PUI in PUIsInBothSources
              # Use the author ID from the mapping file.
              # chage ciation records
              for s in record.split('\1'):
                if s.startswith("CI2"):
                  #names_data = s[3:].split(';')
                  names_data = re.split(regex_pattern, s[3:])
                  if len(names_data) != len(PUIsInBothSources[pui_data]):
                    print ("WARNING: INCONSISTENT Found !", pui_data, names_data, PUIsInBothSources[pui_data])
                  else:
                    newfacts=''
                    for index, name in enumerate(names_data):
                      #print ("new data: {name}\t{PUIsInBothSources[pui_data][index]}")
                      newfacts=f"{newfacts}\1CI2{name}\1CJI{PUIsInBothSources[pui_data][index]}"
                    #print ("realy nice:", newfacts[1:])
                    record = '\1'.join([newfacts[1:] if s.startswith("CI2") else s for s in record.split('\1')])
                    #print ("record:", record)
            else:
              # if there's no PUI for this citation
              if fact.startswith('CI2'):
                print ("WARNING: NO PUI Found!", record)
                names_data = fact[3:].split(';')
                newfacts=''
                for name in names_data:
                  for name in names_data:
                    if name in NewAuthors:
                      newfacts=f"{newfacts}\1CI2{name}\1CJI{NewAuthors[name]}"
                    else:
                      ################# to be checked !! #######################
                      newfacts=f"{newfacts}\1CI2{name}"
                      # chage ciation records
                    record = '\1'.join([newfacts[1:] if s.startswith("CI2") else s for s in record.split('\1')])
                  else:
                    sir_name, given_name = [x.strip() for x in name.split(',')]
                    sir_name=f"\1AU5{given_name}"
                    if given_name is None:
                      given_name=f"\1AU6{given_name}"
                    maxPrimaryKey += 1
                    maxAuthorID +=1
                    newAuRecord= f"\1AU1{maxPrimaryKey}\1AU2{maxAuthorID}{given_name}{sir_name}"
                    reducedAuthorContext.append(newAuRecord)
                    NewAuthors[name]=maxAuthorID
                    print ("INFO: NEW AUTHOR Found !", name, maxAuthorID)
        newRecords.append(record)
  if newRecords[-1] == "":
    del newRecords[-1]
  print (len(reducedAuthorContext))
  print (len(newRecords))
  # Open a file in write mode
  with open(f"{z_f}.new", "w", encoding="latin-1") as file:
    for index, record in enumerate(newRecords):
      # Write the item to the file
      file.write(record)
      # Write the separator, except for the last item
      file.write("\2")



NewAuthors = []
zit_files = glob.glob(os.path.join(DATA, 'cit*.xf'))
for z_f in zit_files:
  newRecords =[]
  write_citation_with_author_id(z_f)


with open(f"{DATA}/aut000.xf.new", "w", encoding="latin-1") as file:
  for index, record in enumerate(reducedAuthorContext):
    # Write the item to the file
    file.write(record)
    # Write the separator, except for the last item
    file.write("\2")


