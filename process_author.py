import csv
import glob
import os
import re
import sys

if len(sys.argv) == 2:
    DATA = sys.argv[1]
else:
    DATA = "/projects/production/reaxys/dbmake/xf110008d"

# this script based on old export using CI2 to generate data
# script description https://elsevier.atlassian.net/browse/RX-10178

# ZIT_XF_FILES = "smalldb/rx100008/fabrication_files/rx100008/xf100008d/cit000.xf"
ZIT_XF_FILES = f"{DATA}/cit*.xf"
AUT_XF_FILES = "/projects/production/auth_back/rawen_out"
PUI_AID_MAPPING_FILE = "/projects/production/auth_back/pui2auth-map.tsv"
NEW_AUTHOR_ID = 0  # start of New Author ID,

maxInt = sys.maxsize
print(maxInt)

while True:
    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt / 10)
print(maxInt)

# First Citation Pass
citationPUIs = {}
zit_files = glob.glob(os.path.join(DATA, "cit*.xf"))
for z_f in zit_files:
    with open(z_f, encoding="latin-1") as xf_file:
        xf_reader = csv.reader(xf_file, delimiter="\2")
        for records in xf_reader:
            for record in records:
                for fact in record.split("\1"):
                    if fact.startswith("R1_"):
                        key_data = fact[3:]
                    # CIZ PUI
                    elif fact.startswith("CIZ"):
                        pui_data = fact[3:]
                        if key_data and pui_data:
                            citationPUIs[pui_data] = int(key_data)
                            key_data = ""
                            pui_data = ""

print(len(citationPUIs))

# Pass Through Mapping File
# Copy all records having an author ID fromUsedAuthorIDs to a new set of author files.
# Remember the highest author ID () and primary key (AU1).
# set
UsedAuthorIDs = set()
PUIsInBothSources = {}

with open(PUI_AID_MAPPING_FILE, encoding="latin-1") as tsv_file:
    tsv_reader = csv.reader(tsv_file, delimiter="\t")
    next(tsv_reader, None)  # skip the headers
    for line in tsv_reader:
        key = line[0]
        if key in citationPUIs:
            UsedAuthorIDs.update(line[1].split(","))
            PUIsInBothSources[key] = line[1].split(",")
        # else:
        #  citationPUIs[key] = line[1]

print(len(UsedAuthorIDs))
# print(UsedAuthorIDs)

# Pass Through Author Context
# Copy all records having an author ID from UsedAuthorIDs to a new set of author files.
reducedAuthorContext = []
min_author_id = 0
min_author_id_record = ""

for file_path in glob.glob(os.path.join(AUT_XF_FILES, "aut*.xf")):
    print(file_path)
    with open(file_path, encoding="latin-1") as xf_file:
        xf_reader = csv.reader(xf_file, delimiter="\2")
        for records in xf_reader:
            for record in records:
                for fact in record.split("\1"):
                    # AU2 is author ID
                    if fact.startswith("AU2"):
                        author_id = fact[3:]
                        if author_id in UsedAuthorIDs:
                            reducedAuthorContext.append(record)
                            if int(author_id) > min_author_id:
                                min_author_id_record = record  # to get AUI later
                            continue  # next record

# Remember the highest author ID () and primary key (AU1).
min_author_id = 0
max_primary_key = 0
print(min_author_id_record)
for fact in min_author_id_record.split("\1"):
    if fact.startswith("AU1"):
        max_primary_key = int(fact[3:])
    elif fact.startswith("AU2"):
        min_author_id = int(fact[3:])
print(min_author_id, max_primary_key)
print(len(reducedAuthorContext))
# print (records)


def add_new_author(name, max_primary_key, min_author_id):
    sub_names = list(filter(None, [x.strip() for x in name.split(",")]))
    sir_name = ""
    given_name = ""
    if len(sub_names) == 1:
        sir_name = sub_names[0]
    elif len(sub_names) == 2:
        sir_name, given_name = sub_names[0], sub_names[1]
    else:
        print(
            f"WARNING 1: Name format is invalid, {name}, expect sir_name or sir_name, given_name"
        )
    if sir_name != "":
        sir_name = f"\1AU6{sir_name}"
    if given_name != "":
        given_name = f"\1AU5{given_name}"
    max_primary_key += 1
    min_author_id -= 1
    newAuRecord = f"\1AU1{max_primary_key}\1AU2{min_author_id}{given_name}{sir_name}"
    reducedAuthorContext.append(newAuRecord)
    new_authors[name] = min_author_id
    print(
        "INFO: NEW AUTHOR Found !",
        name,
        min_author_id,
    )
    return max_primary_key, min_author_id


def write_citation_with_author_id(z_f, max_primary_key, min_author_id):
    newRecords = []
    # regex_pattern = r'(?<!&#x[\da-zA-Z]{4});(?!#)'
    regex_pattern = r"(?<!&#x[\da-fA-F]{4})(?<!&#[\d]{3});(?!#)"
    with open(z_f, encoding="latin-1") as xf_file:
        xf_reader = csv.reader(xf_file, delimiter="\2", skipinitialspace=True)
        for records in xf_reader:
            for record in records:
                for fact in record.split("\1"):
                    # CIZ PUI
                    if fact.startswith("CIZ"):
                        pui_data = fact[3:]
                        if pui_data in PUIsInBothSources:
                            # If the the citation has a PUI in PUIsInBothSources
                            # Use the author ID from the mapping file.
                            # change citation records
                            for s in record.split("\1"):
                                if s.startswith("CI2"):
                                    # names_data = s[3:].split(';')
                                    names_data = re.split(regex_pattern, s[3:])
                                    if len(names_data) != len(
                                        PUIsInBothSources[pui_data]
                                    ):
                                        print(
                                            "WARNING: INCONSISTENT Found !",
                                            pui_data,
                                            names_data,
                                            PUIsInBothSources[pui_data],
                                        )
                                        for name in names_data:
                                            (
                                                max_primary_key,
                                                min_author_id,
                                            ) = add_new_author(
                                                name, max_primary_key, min_author_id
                                            )
                                    else:
                                        new_facts = s
                                        for index, name in enumerate(names_data):
                                            # print ("new data: {name}\t{PUIsInBothSources[pui_data][index]}")
                                            new_facts = f"{new_facts}\1CJI{name}\1CJJ{PUIsInBothSources[pui_data][index]}"
                                        # print ("really nice:", new_facts[1:])
                                        record = "\1".join(
                                            [
                                                new_facts if s.startswith("CI2") else s
                                                for s in record.split("\1")
                                            ]
                                        )
                                        # print ("record:", record)
                        else:
                            # if there's no PUI for this citation
                            if fact.startswith("CI2"):
                                print("WARNING: NO PUI Found!", record)
                                names_data = fact[3:].split(";")
                                new_facts = fact
                                for name in names_data:
                                    for name in names_data:
                                        if name in new_authors:
                                            new_facts = f"{new_facts}\1CJI{name}\1CJJ{new_authors[name]}"
                                        else:
                                            ################# to be checked !! #######################
                                            new_facts = f"{new_facts}\1CJI{name}"
                                            # change citation records
                                        record = "\1".join(
                                            [
                                                new_facts if s.startswith("CI2") else s
                                                for s in record.split("\1")
                                            ]
                                        )
                                    else:
                                        max_primary_key, min_author_id = add_new_author(
                                            name, max_primary_key, min_author_id
                                        )

                newRecords.append(record)
    if newRecords[-1] == "":
        del newRecords[-1]
    print(len(reducedAuthorContext))
    print(len(newRecords))
    # Open a file in write mode
    with open(f"{z_f}.new", "w", encoding="latin-1") as file:
        for index, record in enumerate(newRecords):
            # Write the item to the file
            file.write(record)
            # Write the separator, except for the last item
            file.write("\2")


new_authors = {}
zit_files = glob.glob(os.path.join(DATA, "cit*.xf"))
for z_f in zit_files:
    newRecords = []
    write_citation_with_author_id(z_f, max_primary_key, NEW_AUTHOR_ID)


with open(f"{DATA}/aut000.xf.new", "w", encoding="latin-1") as file:
    for index, record in enumerate(reducedAuthorContext):
        # Write the item to the file
        file.write(record)
        # Write the separator, except for the last item
        file.write("\2")
