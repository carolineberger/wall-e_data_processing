import pandas
import pathlib
import sys
import global_constants as gc
from LikertResponse import LikertResponse
from MessageResponse import MessageResponse
from NamedDataFrame import NamedDataFrame
from process import clean_data_path

def clean_columns(raw_data_file_paths):
    """""
    Massage data into an organized form
    :param raw_data_file_paths: raw data folder path
    :type raw_data_file_paths: path
    :returns clean data
    :rtype [NamedDataFrame]
    """""
    clean_data = []
    rows = []
    #file by file
    for raw_file in raw_data_file_paths:
        df = pandas.read_csv(raw_file)
        ## ATTENTION CHECKS
        df = attention_checks(df)
        # REMOVE ATTNCHECKS AND SHOW_INST_1
        df = df[df['trlid'] != "ATTN_1"]
        df = df[df['trlid'] != "ATTN_2"]
        df = df[df['trlid'] != "Show_Inst_1"]

        likert_responses, msg_responses = create_internal_data_types(df, rows)



        first_sub_id = get_first_sub_id(rows)

        new_columns = make_columns(msg_responses, first_sub_id)
        likert_responses.sort(key=lambda x: x.associated_msg)
        fill_rows(likert_responses, msg_responses, rows)
        # reindex forces the correct ordering of the columns
        new_df = pandas.DataFrame(data= rows, columns=new_columns).reindex(columns=new_columns)
        clean_data.append(NamedDataFrame(raw_file.name, new_df))

    return clean_data

def attention_checks(df):
    attention_chk_info_path = clean_data_path / "Attention_Check_Results.txt"
    f = open(attention_chk_info_path, "w")

    (msg_per_block_count, msg_likert_count, block_likert_count, attention_check_1_ans, attention_check_2_ans) = get_param_info()
    for ind in df.index:
        if df['trlid'][ind] == "ATTN_1":
            if (str(df['key1'][ind]) != str(attention_check_1_ans)):
                f.write("ATTENTION CHECK FAILURE.\nParticipant: " + df['SubjectID'][
                    ind] + "\nAttention check: 1\nExpected input: " + str(
                    attention_check_1_ans) + ". \nParticipant input: " + str(df['key1'][ind]))
        elif df['trlid'][ind] == "ATTN_2":
            if (str(df['key1'][ind]) != str(attention_check_1_ans)):
                f.write("ATTENTION CHECK FAILURE.\nParticipant: " + df['SubjectID'][ind] + "\nAttention check: 2\nExpected input: " + str(
                    attention_check_2_ans) + ". \nParticipant input: " + str(df['key1'][ind]))
    f.close()
    return df

def get_first_sub_id(rows):
    """"
    Get the frist subjectid
    :param rows: rows for the clean dataframe
    :type rows: [{}]
    :returns first_sub_id
    :rtype str
    """""
    first_sub_id = ''
    try:
        if rows[0]['SubjectID']:
            first_sub_id = rows[0]['SubjectID']
    except IndexError:
        print("Error! All rows must have a SubjectID")
        sys.exit()
    return first_sub_id


def create_internal_data_types(df, rows):
    """"
    Manages the creation of the message responses and likert responses
    :param df: dataframe to base message and likert responses on
    :type df: pandas dataframe
    :param rows: rows to go into the clean data frame
    :type rows: [{}]
    :return likert_responses, msg_responses
    :rtype ([LikertResponse], [MessageResponse])
    """
    msg_responses = []
    likert_responses = []
    # list of rows
    for ind in df.index:
        # populate row list
        row = {'SubjectID': df['SubjectID'][ind]}

        if row not in rows:
            rows.append(row)
        create_message_responses(df, ind, msg_responses)
        create_likert_responses(df, ind, likert_responses)

    # sort first by subject id, then by message id
    msg_responses.sort(key=lambda x: (x.subject_id, x.msg_id))
    likert_responses.sort(key=lambda x: (x.subject_id, x.trial_id))
    return likert_responses, msg_responses


def fill_rows(likert_responses, msg_responses, rows):
    """""
    Fill rows with message responses and likert responses
    :param likert_responses: complete list of all likert responses
    :type likert_responses: [LikertResponse]
    :param msg_responses: complete list of all message responses
    :type msg_responses: [MessageResponse]
    :param rows: rows for dataframe
    :type rows: [{}]
    """
    for row in rows:
        for msg_response in msg_responses:
            if row['SubjectID'] == msg_response.subject_id:
                row[msg_response.msg_id + "_Response"] = msg_response.answer
                row[msg_response.msg_id + "_Response_Time"] = msg_response.resp_time
    for row in rows:
        for l_response in likert_responses:
            if l_response.associated_msg != "System" and row['SubjectID'] == l_response.subject_id:
                row[l_response.associated_msg + "_" + l_response.likert_code + "_Response"] = l_response.answer
            elif l_response.associated_msg == "System" and row['SubjectID'] == l_response.subject_id:
                row[l_response.trial_id + "_" + l_response.likert_code + "_Response"] = l_response.answer


def make_columns(msg_responses, first_sub_id):
    """""
    Create a list for the columns
    :param msg_responses: list of message responses to create columns based on
    :type msg_responses: [MessageResponse]
    :param first_sub_id: first subject id to appear
    :type first_sub_id: str
    """""
    (msg_per_block_count, msg_likert_count, block_likert_count, attention_check_1_ans, attention_check_2_ans) = get_param_info()
    new_columns = []
    new_columns.append('SubjectID')
    for i in range(len(msg_responses)):
        if msg_responses[i].subject_id == first_sub_id:
            new_columns.append(msg_responses[i].msg_id + "_Response")
            new_columns.append(msg_responses[i].msg_id + "_Response_Time")
            for lm in range(1, (msg_likert_count+1)):
                new_columns.append(msg_responses[i].msg_id + "_LM" + str(lm) +"_Response")
            if i != 0 and (i - 2) % 3 == 0:
                for lb in range(1, (block_likert_count + 1)):
                    new_columns.append(msg_responses[i].trial_id + "_LB"+ str(lb)+"_Response")
    return new_columns


def create_message_responses(df, ind, msg_responses):
    """""
    Append to message response list
    :param df: dataframe to base new message responses on 
    :type df: pandas dataframe
    :param ind: index in dataframe
    :type ind: int
    :param msg_responses: list of all message responses
    :type msg_responses: [MessageResponse]
    """""
    (msg_per_block_count, msg_likert_count, block_likert_count, attention_check_1_ans, attention_check_2_ans) = get_param_info()
    key_rt_num = 1
    blk_v = 3
    for m in range(1, (msg_per_block_count + 1)):
        msg_responses.append(
            MessageResponse(df['SubjectID'][ind], df['trlid'][ind], df['key' + str(key_rt_num)][ind], df['rt'+ str(key_rt_num)][ind],
                            df['msgblk_v' + str(blk_v)][ind]))
        key_rt_num = key_rt_num + 4
        blk_v = blk_v + 3



def create_likert_responses(df, ind, likert_response):
    """""
    Append to the likert response list 
    :param df: dataframe to base new likert responses on 
    :type df: pandas dataframe
    :param ind: index in dataframe
    :type ind: int
    :param likert_response: list of all likert_responses
    :type likert_response: [LikertResponse]
    """""
    (msg_per_block_count, msg_likert_count, block_likert_count, attention_check_1_ans,
                    attention_check_2_ans) = get_param_info()
    key_num = 2
    blk_v = 3
    for msgs in range(1, (msg_per_block_count+1)):
        for lmr in range(1, (msg_likert_count + 1)):
            likert_response.append(
                LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LM'+str(lmr), df['key'+str(key_num)][ind],
                                df['msgblk_v'+str(blk_v)][ind]))
            key_num = key_num + 1
        key_num = key_num + 1
        blk_v = blk_v + 3

    # key_num = #msg * likert_count + msg_responses (a, a, b) + 1 (next index)
    key_num = (msg_per_block_count * msg_likert_count) + msg_per_block_count + 1
    for lbr in range(1, (block_likert_count+1)):
        likert_response.append(
            LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LB'+str(lbr), df['key'+str(key_num)][ind]))
        key_num = key_num + 1

def write_csv(cleaned_data):
    """""
    for each frame, write a csv to the clean data path
    :param cleaned_data: list of cleaned data frames    
    :type cleaned_data: [pandas.dataframe]
    """""
    for named_frame in cleaned_data:
        # create a new csv in the clean_data_path directory, with the same name
        # as the original file
        path = clean_data_path / named_frame.name
        frame = named_frame.df
        frame_index = frame.index.name
        frame.to_csv(path, index=frame_index, header=True)

def get_param_info():
    """"
    based on the raw file info folder, returns  information
    on the number of blocks, the number of messages per block,
    the number of likert questions per message, and the number
    of likert questions per block.
    :raises prints an error if parameters file does not match what it is expecting
    :rtype: (int, int, int, int)
    :return: [msg_per_block_count, msg_likert_count, block_likert_count, attention_check_1_ans,
                    attention_check_2_ans]
    """
    raw_file_info_path = pathlib.Path().absolute() / gc.RAW_FILE_INFO_FOLDER_NAME
    for info_path in raw_file_info_path.rglob("*.csv"):
        if info_path.name == gc.PARAMETER_FILE_NAME:
            df = pandas.read_csv(info_path)
        try:
            # initialize global varaibles
            param_dict = df.to_dict()
            msg_per_block_count = init_params(param_dict, 'MessagePerBlockCount')
            msg_likert_count = init_params(param_dict, 'MessageLikertCount')
            block_likert_count = init_params(param_dict, 'AttnChk1')
            attention_check_1_ans = init_params(param_dict, 'AttnChk1')
            attention_check_2_ans = init_params(param_dict, 'AttnChk2')
            return [msg_per_block_count, msg_likert_count, block_likert_count, attention_check_1_ans,
                    attention_check_2_ans]
        except:
            print("Error in " + info_path.name + ".\nYou are not allowed to change the column names or add more columns.\nMake sure there are no white spaces between comma seperated elements.")

def init_params(param_dict, elem_str):
    """"
    help to get the int value of element
    :param param_dict: parameters data frame dictionary
    :type param_dict: dict
    :param elem_str: column name
    :type elem_str: str
    :rtype: int
    :return: value of the passed column
    """
    tmp = param_dict[elem_str]
    param = tmp[0]
    return param

