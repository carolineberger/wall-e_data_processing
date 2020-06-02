import csv
import pathlib
import pandas
import global_constants as gc

# for print statement
pandas.set_option('display.max_rows', 500)
pandas.set_option('display.max_columns', 500)
pandas.set_option('display.width', 1000)

raw_data_path = pathlib.Path().absolute() / gc.RAW_DATA_FOLDER_NAME
raw_data = []

for f in raw_data_path.rglob("*.csv"):
    raw_data.append(f)


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


def clean_columns(raw_data, to_drop):
    # list of all the clean data data frames
    clean_data = []
    for f in raw_data:
        df = pandas.read_csv(f)
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
        block_likerts_filtered = filter(lambda x: x.associated_msg == "System", likert_responses)
        block_likert = zip(*(iter(block_likerts_filtered),) * 7)        ## 2D array


        new_columns = []
        new_columns.append('SubjectID')
        for i in range(len(msg_responses)):
            new_columns.append(msg_responses[i].msg_id + "_" + "Response")
            new_columns.append(msg_responses[i].msg_id + "_" + "Time")
            new_columns.append(msg_responses[i].msg_id + "_L1_Response")
            new_columns.append(msg_responses[i].msg_id + "_L2_Response")
            new_columns.append(msg_responses[i].msg_id + "_L3_Response")
            if i != 0 and (i - 2) % 3 == 0:
                new_columns.append(msg_responses[i].trial_id + "_L4_Response")
                new_columns.append(msg_responses[i].trial_id + "_L5_Response")
                new_columns.append(msg_responses[i].trial_id + "_L6_Response")
                new_columns.append(msg_responses[i].trial_id + "_L7_Response")
                new_columns.append(msg_responses[i].trial_id + "_L8_Response")
                new_columns.append(msg_responses[i].trial_id + "_L9_Response")


        new_df = pandas.DataFrame(columns=new_columns)

        clean_data.append(NamedDataFrame(f.name, new_df))

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
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'L1', df['key2'][ind],
                       df['rt2'][ind], df['msgblk_v3'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'L2', df['key3'][ind],
                       df['rt3'][ind], df['msgblk_v3'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'L3', df['key4'][ind],
                       df['rt4'][ind], df['msgblk_v3'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'L4', df['key6'][ind],
                       df['rt6'][ind], df['msgblk_v6'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'L5', df['key7'][ind],
                       df['rt7'][ind], df['msgblk_v6'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'L6', df['key8'][ind],
                       df['rt8'][ind], df['msgblk_v6'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'L7', df['key10'][ind],
                       df['rt10'][ind], df['msgblk_v9'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'L8', df['key11'][ind],
                       df['rt11'][ind], df['msgblk_v9'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'L9', df['key12'][ind],
                       df['rt12'][ind], df['msgblk_v9'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'L10', df['key13'][ind],
                       df['rt13'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'L11', df['key14'][ind],
                       df['rt14'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'L12', df['key15'][ind],
                       df['rt15'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'L13', df['key16'][ind],
                       df['rt16'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'L14', df['key17'][ind],
                       df['rt17'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'L15', df['key18'][ind],
                       df['rt18'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'L16', df['key19'][ind],
                       df['rt19'][ind]))


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

cleaned_data = clean_columns(raw_data, to_drop)


def write_csv(cleaned_data):
    for named_frame in cleaned_data:
        # create a new csv in the clean_data_path directory, with the same name
        # as the original file
        path = clean_data_path / named_frame.name
        frame = named_frame.df
        frame_index = frame.index.name

        frame.to_csv(path, index=frame_index, header=True)


write_csv(cleaned_data)
