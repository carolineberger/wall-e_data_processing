import pandas
import pathlib
import global_constants as gc
from LikertResponse import LikertResponse
from MessageResponse import MessageResponse
from NamedDataFrame import NamedDataFrame
from process import clean_data_path

def clean_columns(raw_data_file_paths):
    clean_data = []
    rows = []
    #file by file
    for raw_file in raw_data_file_paths:
        df = pandas.read_csv(raw_file)
        likert_responses, msg_responses = create_internal_data_types(df, rows)
        first_sub_id = ''
        if rows[0]['SubjectID']:
            first_sub_id = rows[0]['SubjectID']
        new_columns = make_columns(msg_responses, first_sub_id)
        likert_responses.sort(key=lambda x: x.associated_msg)

        fill_rows(likert_responses, msg_responses, rows)

        # reindex forces the correct ordering of the columns
        new_df = pandas.DataFrame(data= rows, columns=new_columns).reindex(columns=new_columns)
        clean_data.append(NamedDataFrame(raw_file.name, new_df))

    return clean_data


def create_internal_data_types(df, rows):
    msg_responses = []
    likert_responses = []
    # list of rows
    for ind in df.index:
        # populate row list
        row = {}
        row['SubjectID'] = df['SubjectID'][ind]
        if row not in rows:
            rows.append(row)
        create_message_responses(df, ind, msg_responses)
        create_likert_responses(df, ind, likert_responses)
    # sort first by subject id, then by message id
    msg_responses.sort(key=lambda x: (x.subject_id, x.msg_id))
    likert_responses.sort(key=lambda x: (x.subject_id, x.trial_id))
    return likert_responses, msg_responses


def fill_rows(likert_responses, msg_responses, rows):
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
    (block_count, msg_per_block_count, msg_likert_count, block_likert_count) = get_param_info()
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
    (block_count, msg_per_block_count, msg_likert_count, block_likert_count) = get_param_info()
    key_rt_num = 1
    blk_v = 3
    for m in range(1, (msg_per_block_count + 1)):
        msg_responses.append(
            MessageResponse(df['SubjectID'][ind], df['trlid'][ind],
                            df['msgblk_v' + str(blk_v)][ind], df['key' + str(key_rt_num)][ind], df['rt'+ str(key_rt_num)][ind]))
        key_rt_num = key_rt_num + 4
        blk_v = blk_v + 3



def create_likert_responses(df, ind, likert_response):
    (block_count, msg_per_block_count, msg_likert_count, block_likert_count) = get_param_info()
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

    # #msg * likert_count + msg_responses (a, a, b) + 1 (next index)
    key_num = (msg_per_block_count * msg_likert_count) + msg_per_block_count + 1
    for lbr in range(1, (block_likert_count+1)):
        likert_response.append(
            LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LB'+str(lbr), df['key'+str(key_num)][ind]))
        key_num = key_num + 1

def write_csv(cleaned_data):
    for named_frame in cleaned_data:
        # create a new csv in the clean_data_path directory, with the same name
        # as the original file
        path = clean_data_path / named_frame.name
        frame = named_frame.df
        frame_index = frame.index.name

        frame.to_csv(path, index=frame_index, header=True)

def get_param_info():
    raw_file_info_path = pathlib.Path().absolute() / gc.RAW_FILE_INFO_FOLDER_NAME
    for info_path in raw_file_info_path.rglob("*.csv"):
        if info_path.name == gc.PARAMETER_FILE_NAME:
            df = pandas.read_csv(info_path)
        try:
            # initialize global varaibles
            param_dict = df.to_dict()
            block_count = init_global_param(param_dict, 'BlockCount')
            msg_per_block_count = init_global_param(param_dict, 'MessagePerBlockCount')
            msg_likert_count = init_global_param(param_dict, 'MessageLikertCount')
            block_likert_count = init_global_param(param_dict, 'BlockLikertCount')

            return block_count, msg_per_block_count, msg_likert_count, block_likert_count
        except:
            print("Error in " + info_path.name + ".\nMake sure there are no white spaces between comma seperated elements.")

def init_global_param(param_dict, elem_str):
    tmp = param_dict[elem_str]
    param = tmp[0]
    return param

