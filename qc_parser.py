
# -*- coding: utf-8 -*-
# ################################### INFO ##################################### #
# QC Parser V1.0
# Author: Amir Shams
# Email: amir.shams84@gmail.com
# Date:	MAR-09-2018
# QC PARSER will search myRTB Sample Attchment folder for RNA Degradation qc report and aggregate them
# python qc_parser.py --myrtb_link "https://myrtb.nih.gov/Microarray/Sample%20Set%20Attachments/Forms/all_docs.aspx" --username nih\\username --password password --outputdir ./
# ################################### IMPORTED LIBRARY ######################### #
import sys  # main system library
import argparse  # parsing input arguments
import requests  # http request
from requests_ntlm import HttpNtlmAuth  # https request
from bs4 import BeautifulSoup  # parsing html file
import pandas  # parisng excel files
import re  # reg-ex 
from collections import OrderedDict  # sorted Dictionary
if sys.version_info[0] < 3:
	reload(sys)
	sys.setdefaultencoding('utf-8')

# ################################### GLOBALS  ################################ #
target_extension_List = ["xls", "xlsx"]
target_pattern = "degradation"

target_header_list = ["sample", "sample_id", "a260", "260/280", "rna_degradation", "pass/fail"]
A_number_pattern = "^[a-zA-Z]{1,2}\d{4}$"
# ################################### FUNCTIONS ################################ #


def get_extension(file_PATH):
	# Get Extension
	try:
		return file_PATH.split('.')[-1].lower()
	except AttributeError:
		return ""


def parse_rna_degradation_xl_file(parsable_excel_HD, unparsable_excel_HD, myrtb_soup, myrtb_address, myrtb_session, outputdir):
	#
	A_number_dataframe_List = []
	
	string = "File_Path" + '\t' + "row_count" + '\t' + "samples_found" + '\n'
	for html_link in myrtb_soup.find_all('a'):
		#
		html_link_href_element = html_link.get('href')
		html_link_href_element_extension = get_extension(html_link_href_element)
		if html_link_href_element_extension.lower() in target_extension_List and target_pattern.lower() in html_link_href_element.lower():
			#
			target_file_path = myrtb_address + html_link_href_element
			print(target_file_path)
			file_Name = target_file_path.split("/")[-1]
			target_object = myrtb_session.get(target_file_path)
			temp_excel = outputdir + "/temp.xlsx"
			f = open(temp_excel, "wb")
			f.write(target_object.content)
			f.close()
		else:
			continue
		raw_target_DF = pandas.read_excel(temp_excel, skip_blank_lines=True, index=False, header=None, encoding='utf-8')
		#raw_target_DF.fillna("????", inplace=True)
		#raw_target_DF = pandas.read_excel(excel_data)
		raw_nrows, raw_ncols = raw_target_DF.shape

		A_number_Dict = OrderedDict()
		A_number_header = ["File_Path", "File_Name"]
		header_flag = False
		row_counter = 0
		for index, row in raw_target_DF.iterrows():
			
			row_counter += 1
			# sluggify row elements
			row_List = []
			potential_head_List = []
			raw_row_List = map(str, row.tolist())
			for each_row_element in raw_row_List:
				#
				row_List.append(each_row_element.strip())
				potential_head_List.append(each_row_element.lower().strip().replace(" ", "_"))
			############################
			# Detecting the header line
			if len(set(potential_head_List).intersection(target_header_list)) > 3 and header_flag is False:
				if header_flag is False:
					A_number_header.extend(row_List)
					header_flag = True
				else:
					continue
			elif len(set(potential_head_List).intersection(target_header_list)) > 0 and header_flag is True:
				#
				break
			############################
			# Parsing element of row list
			for each_element in row_List:
				#
				A_number_match_test = re.search(A_number_pattern, each_element)
				if A_number_match_test:
					A_number = A_number_match_test.group(0).strip().lower()
					if A_number not in A_number_Dict:
						row_List.insert(0, target_file_path)
						file_Name = target_file_path.split("/")[-1]
						row_List.insert(1, file_Name)
						A_number_Dict[A_number] = dict(zip(A_number_header, row_List))
						break
					else:
						pass
				else:
					pass
			else:
				pass
			###########################
		else:
			pass
		A_number_dataframe = pandas.DataFrame.from_dict(A_number_Dict, orient='index')
		string += str(target_file_path) + '\t' + str(row_counter) + '\t' + str(len(A_number_Dict.keys())) + '\n'
		if len(A_number_Dict.keys()) == 0:
			raw_target_DF.to_excel(unparsable_excel_HD, sheet_name=file_Name[:30], index=False, startrow=0, startcol=0)
		else:
			A_number_dataframe_List.append(A_number_dataframe)
	else:
		pass
	QC_dataframe = pandas.DataFrame()
	QC_dataframe = QC_dataframe.append(A_number_dataframe_List, ignore_index=True)
	QC_dataframe.set_index(["File_Path"], drop=True, inplace=True)			
	QC_dataframe.to_excel(parsable_excel_HD, sheet_name="RNA_degradation_report", index=True, startrow=0, startcol=0)
	o = open(outputdir + "/qc_parser_report.txt", "w")
	o.write(string)
	o.close()
	return True


def myrtb_authentication(username, password):
	#
	myrtb_session = requests.Session()
	myrtb_session.auth = HttpNtlmAuth(username, password)
	return myrtb_session


def main(argv):
	# ++++++++++++++++++++++++++++++ PARSE INPUT ARGUMENTS
	parser = argparse.ArgumentParser()
	main_file = parser.add_argument_group('Main file parameters')
	main_file.add_argument("--myrtb_link", help="myRTB html link to parse", required=True)
	main_file.add_argument("--outputdir", help="output directory for results", required=True)
	main_file.add_argument("--username", help="username for myRTB authentication", required=True)
	main_file.add_argument("--password", help="password for myRTB authentication", required=True)
	args = parser.parse_args()
	# ------------------------------ END OF PARSE INPUT ARGUMENTS
	myrtb_session = myrtb_authentication(args.username, args.password)
	myrtb_address = "https://myrtb.nih.gov"
	html_link = args.myrtb_link

	myrtb_html_object = myrtb_session.get(html_link)
	
	myrtb_soup = BeautifulSoup(myrtb_html_object.content, 'html.parser')

	parsable_excel_file_Path = args.outputdir + '/rna_degradation_report.xlsx'
	parsable_excel_HD = pandas.ExcelWriter(parsable_excel_file_Path)
	unparsable_excel_file_Path = args.outputdir + '/unparsable_rna_degradation_report.xlsx'
	unparsable_excel_HD = pandas.ExcelWriter(unparsable_excel_file_Path)
	parse_rna_degradation_xl_file(parsable_excel_HD, unparsable_excel_HD, myrtb_soup, myrtb_address, myrtb_session, args.outputdir)
	parsable_excel_HD.save()
	unparsable_excel_HD.save()
	return True

# ################################### FINITO ################################### #
if __name__ == "__main__":
	main(sys.argv[1:])