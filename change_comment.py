import re,os
from collections import namedtuple

def check_comment(full_string,index):
	#Check if index is in a comment line or not
	is_comment = 0
	for i in range(index,0):
		if full_string[i] == "\n":
			break
		if full_string[i] == "#":
			is_comment = 1;
	return is_comment
def find_all(a_string, sub):
    result = []
    k = 0
    while k < len(a_string):
        k = a_string.find(sub, k)
        if k == -1:
            return result
        else:
            result.append(k)
            k += 1 #change to k += len(sub) to not search overlapping results
    return result
def check_variable_in_used(func_body,variable):
	list_index = find_all(func_body,variable)
	if len(list_index) == 0:
		return 0 #Not use
	for index in list_index:
		if check_comment(func_body,index) == 1:
			continue
		index3 = index + 1
		for index2 in range(index+1, len(func_body)):
			char = func_body[index2]
			if (char.isalnum() or char == '_'):
				index3 = index2
			else:
				break
		var_name = func_body[index:index3 + 1]
		if var_name == variable:
			return 1
	return 0
def get_log(log_line):
	m = re.search("write_log(.*?);",log_line)
	if m != None:
    		
		log = m.group(1)
		log = log.replace('\",\"',' ');
		log = log.replace('(\"','')
		log = log.replace(')\"','')
		log = log.replace(')','')
		log = log.replace('(','')
		log = log.replace('".','')
		return log
	return ""
def get_return(func_body):
	result=[]
	lines = func_body.split('\n')
	p= re.compile('^return (.*?)')
	print len(lines)
	ReturnValue = namedtuple('ReturnValue', 'value log')
	log=""
	pre_line=""
	for line in lines:
		line = line.replace('\t','')
		m= re.search('return (.*?);',line)
		#print m.group()
		#p = re.compile('(a)b')
		#m = p.match('ab')
		if m != None:
			#print m.group(1)
			a =ReturnValue(value=m.group(1), log=get_log(pre_line))
			print a 
			#result.append(m.group(1))
			result.append(a)
		pre_line = line
	return result
def get_var(func_body):
	result = []
	lines = func_body.split('\n')
	for line in lines:
		line = line.replace('\t','')
		m = re.search('my \((.*?)\) = @',line)
		if m != None:
			var= m.group(1)
			for v in var.split(','):
				result.append(v)
	return result
perl_file = "utilities.pm"
perl_file_outout="out_"+perl_file
with open(perl_file, 'r') as myfile:
	data=myfile.read()
full_statement = ""
with open(perl_file, 'r') as myfile:
	data=myfile.readlines()
	for line in data:
		line2 = line.strip()
		#if(not line2.startswith("#")):
		full_statement = full_statement + line
count = 0
start_block = 0
start_index = 0
finish_index = 0
list_body = []
list_function_name = []
list_start_index = []
for i in range(0, len(full_statement)):
	if (full_statement[i] == "{" and check_comment(full_statement,i) == 0):
		if count == 0:
			start_block = 1
			start_index = i + 1
			beginning_to_func_name =  full_statement[finish_index:i]
			beginning_to_func_name = beginning_to_func_name.rstrip()
			func_name_token = re.split('\s+', beginning_to_func_name)
			func_name = func_name_token[-1]
			list_function_name.append(func_name)
		count = count + 1
	if (full_statement[i] == "}" and check_comment(full_statement,i) == 0):
		count = count - 1
		if (count == 0):
			start_block = 0
			finish_index = i
			print start_index
			print finish_index
			block = full_statement[start_index:finish_index]	
			list_body.append(block)
string_global1 =  """%management_params, @list_master_ldisk, @list_backup_ldisk, @list_datastore_ldisk, 
	@list_rdm_ldisk, @created_lock_files, %ip_wwns, %original_datastores_name, 
	%original_datastores_ldisk, %current_datastores_name, %backup_datastores_name,
	$current_vm_name, %target_vm_info, %target_datastore_info, %current_vm_info, 
	@list_preserved_ldisk_rdms, %master_backup_ldisk, @list_preserved_files, 
	@list_preserved_directory, $current_config_path, $preserved_config_path,
	$current_datastore_name, @list_vms_stop, %preserved_vms_info, %preserved_datastore_info, 
	@list_vms_remove, @list_moved_files, @list_unregistered_vms, %vm_name_config_path, 
	$restore_preserved_datastore, %preserved_mapping_file_ldisk, $host_ip_current_vm,
	$return_code,"""
string_global2 = """$job_id,$job_name, $seq_num, $v_retry_int, $v_retry_num, $concurrency, $quiesce,
	$backup_method, $backup_type, $retention, $datacenter, $backup_home, $tsu_home,$tsr_home, 
	$job_file, $config_file, $vcenter_ip, $vi_user, $cred_file, @storage_ips,
	$is_preserved, $target_name, $current_num, $management_file, $lock_home,"""
	
list_variable_meaning = {'$a':'this is test variable','$b':'var second'}
############## Init variable ########
	
	
string_global = string_global1 + string_global2
string_tokens = string_global.split(",")
list_global = []
for string in string_tokens:
	string = string.strip()
	if string != "":
		list_global.append(string)

list_global_result = {}
for index in range(len(list_body)):
	all_global = []
	body = list_body[index]
	func_name = list_function_name[index]
	for global_variable in list_global:
		print global_variable
		if check_variable_in_used(body,global_variable) == 1:
			all_global.append(global_variable)
			continue
		global_variable2 = ""
		if global_variable[0] != "$":
			global_variable2 = global_variable[1:]
			global_variable2 = "$" + global_variable2
			if check_variable_in_used(body,global_variable2) == 1:
				all_global.append(global_variable)
	list_global_result[func_name] = all_global
try:
	os.remove(perl_file_outout)
except OSError:
	pass			
text_file = open(perl_file_outout, "a+")		
for index in range(len(list_body)):
	body = list_body[index]
	func_name = list_function_name[index]
	list_global_val = list_global_result[func_name]

	#text_file.write("######################################################\n\n")
	text_file.write("#** @function public " + func_name)
	text_file.write("\n# Brief Description: ")
	text_file.write("\n#")
	text_file.write("\n# Detail Description: ")
	text_file.write("\n#\n#\n#")
	text_file.write("\n# Input Parameter: ")
	for global_var in list_global_val:
		if (global_var[0] == "$"):
			tmp_var = "scalar " + global_var[1:]
		elif (global_var[0] == "@"):
			tmp_var = "array " + global_var[1:]
		else:
			tmp_var = "hash " + global_var[1:]
		text_file.write("\n# @params " + tmp_var + " (global):")
	my_var = get_var(body)
	for var in my_var:
		print var
		text_file.write("\n# @params "+var+" :")
		des = list_variable_meaning.get(var)
		if des != None:
			text_file.write(list_variable_meaning.get(var)+".")
	text_file.write("\n# Output:")
	
	return_val=get_return(body)
	for val in return_val:
		text_file.write("\n# @retval value "+val.value)
		if val.log != '':
			text_file.write(' and write message "'+val.log+'" to log file')
		text_file.write('.')
	text_file.write("\n#*")
	#text_file.write("\n######################################################\n")
	text_file.write("\n\nsub " + func_name + " {")
	text_file.write(body + "}\n\n")
	
text_file.close()



