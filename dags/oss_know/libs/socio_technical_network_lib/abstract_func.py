# import os
import sys
import subprocess
from xml.etree import ElementTree as ET
import json
from oss_know.libs.util.log import logger
from oss_know.libs.socio_technical_network_lib.interact_with_opensearch import put_intermediate_data_to_opensearch

file_dir = "/opt/workspace/DoxygenResult/RepoXml/"
file_pre = file_dir + "xml/"
file_post = ".xml"
OPENSEARCH_DOXYGEN_RESULT = "doxygen_result"


def get_subfilename(index_file: str):
    sub_filenames = []
    tree = ET.parse(index_file)
    root = tree.getroot()
    compounds = root.findall("compound")
    for compound in compounds:
        # print(compound.attrib['kind'])
        if compound.attrib['kind'] == "file":
            sub_filenames.append(compound.attrib['refid'])

    return sub_filenames


def get_file_key(repo, absolute_file_key):
    absolute_path = absolute_file_key.split('/')
    for i, path in enumerate(absolute_path):
        if path == repo:
            relative_path = absolute_path[i:]
    return '/'.join(relative_path)


def get_func_info(repo, compounddefs):
    file_key = get_file_key(repo, compounddefs[0].find("location").attrib['file'])
    # func_to_lines = {file_key: {}}
    # lines_to_func = {file_key: {}}
    lines_to_func = []
    func_filenames = []
    for compounddef in compounddefs:
        innerclasses = compounddef.findall("innerclass")
        for innerclass in innerclasses:
            # class_filenames.append(innerclass.attrib['refid'])
            func_filenames.append(innerclass.attrib['refid'])
        innernamespaces = compounddef.findall("innernamespace")
        for innernamespace in innernamespaces:
            # namespace_filenames.append(innernamespace.attrib['refid'])
            func_filenames.append(innernamespace.attrib['refid'])
        sectiondefs = compounddef.findall("sectiondef")
        for sectiondef in sectiondefs:
            kind = sectiondef.attrib['kind']
            if kind.find("func") != -1:
                memberdefs = sectiondef.findall("memberdef")
                for memberdef in memberdefs:
                    if memberdef.attrib['kind'] == "function":
                        func_name = memberdef.find("name").text
                        try:
                            start_line = memberdef.find("location").attrib['bodystart']
                            end_line = memberdef.find("location").attrib['bodyend']
                        except Exception:
                            start_line = end_line = memberdef.find("location").attrib['line']
                        # func_to_lines[file_key][func_name] = {"start_line": start_line, "end_line": end_line}
                        for i in range(int(start_line), int(end_line) + 1):
                            lines_to_func.append({"file_path": file_key, "lines": i, "func": func_name})
                            # lines_to_func[file_key][i] = func_name

    for index, func_filename in enumerate(func_filenames):
        try:
            tree = ET.parse(file_pre + func_filename + file_post)
        except Exception as e:
            logger.debug(e)
            logger.info(file_pre + func_filename + file_post)
            continue
        root = tree.getroot()
        compounddefs = root.findall("compounddef")
        for compounddef in compounddefs:
            sectiondefs = compounddef.findall("sectiondef")
            for sectiondef in sectiondefs:
                kind = sectiondef.attrib['kind']
                if kind.find("func") != -1:
                    memberdefs = sectiondef.findall("memberdef")
                    for memberdef in memberdefs:
                        if memberdef.attrib['kind'] == "function":
                            func_name = memberdef.find("name").text
                            try:
                                start_line = memberdef.find("location").attrib['bodystart']
                                end_line = memberdef.find("location").attrib['bodyend']
                            except Exception:
                                start_line = end_line = memberdef.find("location").attrib['line']
                            # func_to_lines[file_key][func_name] = {"start_line": start_line, "end_line": end_line}
                            for i in range(int(start_line), int(end_line) + 1):
                                lines_to_func.append({"file_path": file_key, "lines": i, "func": func_name})
                                # lines_to_func[file_key][i] = func_name

    return lines_to_func


def get_func_info_py(repo, compounddefs):
    for compounddef in compounddefs:
        # class_filenames, namespace_filenames = [], []
        func_filenames = []
        innerclasses = compounddef.findall("innerclass")
        for innerclass in innerclasses:
            # class_filenames.append(innerclass.attrib['refid'])
            func_filenames.append(innerclass.attrib['refid'])
        innernamespaces = compounddef.findall("innernamespace")
        for innernamespace in innernamespaces:
            # namespace_filenames.append(innernamespace.attrib['refid'])
            func_filenames.append(innernamespace.attrib['refid'])
        file_key = get_file_key(repo, compounddef.find("location").attrib['file'])

    # print(class_filenames, namespace_filenames)
    # func_to_lines = {file_key: {}}
    # lines_to_func = {file_key: {}}
    lines_to_func = []
    # for index, namespace_filename in enumerate(namespace_filenames):
    for index, func_filename in enumerate(func_filenames):
        # if len(func_filenames) > 1 and index == 0:  # Need properer method to process functions of __init__.py
        #     continue
        tree = ET.parse(file_pre + func_filename + file_post)
        root = tree.getroot()
        compounddefs = root.findall("compounddef")
        for compounddef in compounddefs:
            sectiondefs = compounddef.findall("sectiondef")
            for sectiondef in sectiondefs:
                kind = sectiondef.attrib['kind']
                if kind.find("func") != -1:
                    memberdefs = sectiondef.findall("memberdef")
                    for memberdef in memberdefs:
                        if memberdef.attrib['kind'] == "function":
                            func_name = memberdef.find("name").text
                            try:
                                start_line = memberdef.find("location").attrib['bodystart']
                                end_line = memberdef.find("location").attrib['bodyend']
                            except Exception:
                                start_line = end_line = memberdef.find("location").attrib['line']
                            # func_to_lines[file_key][func_name] = {"start_line": start_line, "end_line": end_line}
                            for i in range(int(start_line), int(end_line) + 1):
                                lines_to_func.append({"file_path": file_key, "lines": i, "func": func_name})

    return lines_to_func


def abstract_func(owner, repo, opensearch_conn_info):
    # Use doxygen to parse GitHub repo files into xml format
    command = "doxygen"
    arg = "oss_know/libs/socio_technical_network_lib/doxygen.cfg"
    # arg = "./doxygen.cfg"

    # suc = os.system(command)
    result = subprocess.run([command, arg], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    if not result.returncode:
        # file_dir = "/opt/workspace/DoxygenResult/RepoXml/"
        # file_pre = file_dir + "xml/"
        # file_post = ".xml"
        sub_filenames = get_subfilename(file_pre + "index.xml")

        # funcs_to_lines = {repo: {}}
        # lines_to_funcs = {repo: {}}
        lines_to_funcs = []
        for sub_filename in sub_filenames:
            tree = ET.parse(file_pre + sub_filename + file_post)
            root = tree.getroot()
            compounddefs = root.findall("compounddef")
            if compounddefs[0].attrib['language'] == "Python":  # Get functions from files written in python
                lines_dict = get_func_info_py(repo, compounddefs)
            else:
                lines_dict = get_func_info(repo, compounddefs) # Get functions from files written in other languages
            # funcs_to_lines[repo].update(func_key)
            # lines_to_funcs[repo].update(lines_key)
            lines_to_funcs.extend(lines_dict)

        put_intermediate_data_to_opensearch(owner, repo, opensearch_conn_info, lines_to_funcs)
        return lines_to_funcs
        # with open(f"TimeSeries/data/{repo}_functions.json", 'w') as f:
        #     json.dump(funcs_to_lines, f, indent=4)
        #
        # with open(f"TimeSeries/data/{repo}_lines_functions.json", 'w') as f:
        #     json.dump(lines_to_funcs, f, indent=4)
    else:
        logger.info(f"Fail to parse repo{repo} using Doxygen")
        logger.debug(result.stderr)
        sys.exit(1)
