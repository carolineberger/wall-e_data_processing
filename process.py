import pathlib
import pandas
import global_constants as gc


def set_print_settings():
    # set window options for print statement
    pandas.set_option('display.max_rows', 500)
    pandas.set_option('display.max_columns', 500)
    pandas.set_option('display.width', 1000)

class NamedDataFrame(object):
    def __init__(self, name, df):
        self.name = name
        self.df = df


class MessageResponse(object):
    def __init__(self, subject_id, trial_id, msg_id, answer, resp_time):
        self.subject_id = subject_id
        self.trial_id = trial_id
        self.msg_id = msg_id
        self.answer = answer
        self.resp_time = resp_time

    def __str__(self):
        return str(self.subject_id) + "_" + str(self.msg_id)


class MessageAndLikert(object):
    def __init__(self, subject_id, trial_id, message_response, likert_responses):
        self.subject_id = subject_id
        self.trial_id = trial_id
        self.message_response = message_response
        self.likert_responses = likert_responses


class LikertResponse(object):
    def __init__(self, subject_id, trial_id, likert_code, answer, resp_time, associated_msg="System"):
        self.subject_id = subject_id
        self.trial_id = trial_id
        self.likert_code = likert_code
        self.answer = answer
        self.resp_time = resp_time
        self.associated_msg = associated_msg

    def __str__(self):
        return str(self.likert_code) + "_" + str(self.trial_id) + "_" + str(self.subject_id)

# TODO: document file
def clean_columns(raw_data_file_paths, to_drop):

    clean_data = []

    for raw_file in raw_data_file_paths:
        df = pandas.read_csv(raw_file)
        df.drop(to_drop, inplace=True, axis=1, errors='ignore')
        msg_responses = []
        likert_responses = []
        for ind in df.index:
            create_message_responses(df, ind, msg_responses)
            create_likert_responses(df, ind, likert_responses)

        msg_responses.sort(key=lambda x: x.msg_id)
        msg_likert = []
        for response in msg_responses:
            for l_response in likert_responses:
                related_likert_messages = []
                if l_response.associated_msg == response.msg_id:
                    related_likert_messages.append(l_response)

            msg_likert.append(
                MessageAndLikert(response.subject_id, response.trial_id, response, related_likert_messages))

        likert_responses.sort(key=lambda x: x.trial_id)

        # subject id should not be hard coded
        dict = {'SubjectID': ['045d6e36']}
        new_columns = []
        #TODO: make subject_id not hard coded
        new_columns.append('SubjectID')
        for i in range(len(msg_responses)):
            if msg_responses[i].subject_id == '045d6e36':
                dict[(msg_responses[i].msg_id + "_Response")] = msg_responses[i].answer
                dict[(msg_responses[i].msg_id+ "_Response_Time")] = msg_responses[i].resp_time
                new_columns.append(msg_responses[i].msg_id + "_Response")
                new_columns.append(msg_responses[i].msg_id + "_Response_Time")
                #TODO: make columns not hard coded probably use % to decide if message is of type likert question 1, 2 or 3
                new_columns.append(msg_responses[i].msg_id + "_LM1_Response")
                new_columns.append(msg_responses[i].msg_id + "_LM1_Response_Time")
                new_columns.append(msg_responses[i].msg_id + "_LM2_Response")
                new_columns.append(msg_responses[i].msg_id + "_LM2_Response_Time")
                new_columns.append(msg_responses[i].msg_id + "_LM3_Response")
                new_columns.append(msg_responses[i].msg_id + "_LM3_Response_Time")

                if i != 0 and (i - 2) % 3 == 0:
                    new_columns.append(msg_responses[i].trial_id + "_LB1_Response")
                    new_columns.append(msg_responses[i].trial_id + "_LB1_Response_Time")
                    new_columns.append(msg_responses[i].trial_id + "_LB2_Response")
                    new_columns.append(msg_responses[i].trial_id + "_LB2_Response_Time")
                    new_columns.append(msg_responses[i].trial_id + "_LB3_Response")
                    new_columns.append(msg_responses[i].trial_id + "_LB3_Response_Time")
                    new_columns.append(msg_responses[i].trial_id + "_LB4_Response")
                    new_columns.append(msg_responses[i].trial_id + "_LB4_Response_Time")
                    new_columns.append(msg_responses[i].trial_id + "_LB5_Response")
                    new_columns.append(msg_responses[i].trial_id + "_LB5_Response_Time")
                    new_columns.append(msg_responses[i].trial_id + "_LB6_Response")
                    new_columns.append(msg_responses[i].trial_id + "_LB6_Response_Time")
                    new_columns.append(msg_responses[i].trial_id + "_LB7_Response")
                    new_columns.append(msg_responses[i].trial_id + "_LB7_Response_Time")
        likert_responses.sort(key=lambda x: x.associated_msg)
        for l_response in likert_responses:
            if l_response.subject_id == '045d6e36' and l_response.associated_msg != "System":
                dict[(l_response.associated_msg + "_"+l_response.likert_code +"_Response")] = l_response.answer
                dict[(l_response.associated_msg + "_"+l_response.likert_code +"_Response_Time")] = l_response.resp_time
            elif l_response.subject_id == '045d6e36' and l_response.associated_msg == "System":
                dict[(l_response.trial_id + "_" + l_response.likert_code + "_Response")] = l_response.answer
                dict[(l_response.trial_id + "_" + l_response.likert_code + "_Response_Time")] = l_response.resp_time

        # reindex forces the correct ordering of the columns
        new_df = pandas.DataFrame(data= dict, columns=new_columns).reindex(columns=new_columns)
        clean_data.append(NamedDataFrame(raw_file.name, new_df))

    return clean_data


def create_message_responses(df, ind, msg_responses):
    msg_responses.append(
        MessageResponse(df['SubjectID'][ind], df['trlid'][ind], df['msgblk_v3'][ind], df['key1'][ind],
                        df['rt1'][ind]))
    msg_responses.append(
        MessageResponse(df['SubjectID'][ind], df['trlid'][ind], df['msgblk_v6'][ind], df['key5'][ind],
                        df['rt5'][ind]))
    msg_responses.append(
        MessageResponse(df['SubjectID'][ind], df['trlid'][ind], df['msgblk_v9'][ind], df['key9'][ind],
                        df['rt9'][ind]))


def create_likert_responses(df, ind, likert_response):
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LM1', df['key2'][ind],
                       df['rt2'][ind], df['msgblk_v3'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LM2', df['key3'][ind],
                       df['rt3'][ind], df['msgblk_v3'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LM3', df['key4'][ind],
                       df['rt4'][ind], df['msgblk_v3'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LM1', df['key6'][ind],
                       df['rt6'][ind], df['msgblk_v6'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LM2', df['key7'][ind],
                       df['rt7'][ind], df['msgblk_v6'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LM3', df['key8'][ind],
                       df['rt8'][ind], df['msgblk_v6'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LM1', df['key10'][ind],
                       df['rt10'][ind], df['msgblk_v9'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LM2', df['key11'][ind],
                       df['rt11'][ind], df['msgblk_v9'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LM3', df['key12'][ind],
                       df['rt12'][ind], df['msgblk_v9'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LB1', df['key13'][ind],
                       df['rt13'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LB2', df['key14'][ind],
                       df['rt14'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LB3', df['key15'][ind],
                       df['rt15'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LB4', df['key16'][ind],
                       df['rt16'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LB5', df['key17'][ind],
                       df['rt17'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LB6', df['key18'][ind],
                       df['rt18'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LB7', df['key19'][ind],
                       df['rt19'][ind]))
def write_csv(cleaned_data):
    for named_frame in cleaned_data:
        # create a new csv in the clean_data_path directory, with the same name
        # as the original file
        path = clean_data_path / named_frame.name
        frame = named_frame.df
        frame_index = frame.index.name

        frame.to_csv(path, index=frame_index, header=True)

set_print_settings()

# populate the raw data path
raw_data_path = pathlib.Path().absolute() / gc.RAW_DATA_FOLDER_NAME
raw_data_file_paths = []

# iterate through the csv files in the raw_data folder
for path in raw_data_path.rglob("*.csv"):
    raw_data_file_paths.append(path)

to_drop = ['Experiment', 'Schedule', 'TestName',
           'MPoint', 'SessionName', 'SessionID', 'LaunchTime',
           'StartTime', 'ResultTime', 'GMTOffset',
           'Exception', 'Remark', 'blktime',
           'blkno', 'block', 'trlno',
           'trial', 'trlspec', 'f1',
           'f2', 'f3', 'f4', 'msgblk_v1', 'msgblk_v2',
           'msgblk_v4', 'msgblk_v5', 'msgblk_v7', 'msgblk_v8',
           'lab1', 'lab2', 'lab3', 'lab4', 'lab5', 'lab6', 'lab7',
           'lab8', 'lab9', 'lab10', 'lab11', 'lab12', 'lab13', 'lab14',
           'lab15', 'lab16', 'lab17', 'lab18', 'lab19'
           ]

clean_data_path = pathlib.Path().absolute() / gc.CLEAN_DATA_FOLDER_NAME

cleaned_data = clean_columns(raw_data_file_paths, to_drop)

write_csv(cleaned_data)
