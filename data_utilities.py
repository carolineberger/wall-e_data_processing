import pandas
from LikertResponse import LikertResponse
from MessageResponse import MessageResponse
from NamedDataFrame import NamedDataFrame
from process import clean_data_path


def clean_columns(raw_data_file_paths, to_drop):
    clean_data = []
    rows = []

    #file by file
    for raw_file in raw_data_file_paths:
        df = pandas.read_csv(raw_file)
        df.drop(to_drop, inplace=True, axis=1, errors='ignore')

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

        #sort first by subject id, then by message id
        msg_responses.sort(key=lambda x: (x.subject_id, x.msg_id))
        likert_responses.sort(key=lambda x: (x.subject_id, x.trial_id))
        # new columns is cycled through 2x, only want once
        new_columns = []
        new_columns.append('SubjectID')
        for i in range(len(msg_responses)):
            if msg_responses[i].subject_id == rows[0]['SubjectID']:
                new_columns.append(msg_responses[i].msg_id + "_Response")
                new_columns.append(msg_responses[i].msg_id + "_Response_Time")
                new_columns.append(msg_responses[i].msg_id + "_LM1_Response")
                new_columns.append(msg_responses[i].msg_id + "_LM2_Response")
                new_columns.append(msg_responses[i].msg_id + "_LM3_Response")
                if i != 0 and (i - 2) % 3 == 0:
                    new_columns.append(msg_responses[i].trial_id + "_LB1_Response")
                    new_columns.append(msg_responses[i].trial_id + "_LB2_Response")
                    new_columns.append(msg_responses[i].trial_id + "_LB3_Response")
                    new_columns.append(msg_responses[i].trial_id + "_LB4_Response")
                    new_columns.append(msg_responses[i].trial_id + "_LB5_Response")
                    new_columns.append(msg_responses[i].trial_id + "_LB6_Response")
                    new_columns.append(msg_responses[i].trial_id + "_LB7_Response")
        likert_responses.sort(key=lambda x: x.associated_msg)

        for row in rows:
            for msg_response in msg_responses:
                if row['SubjectID'] == msg_response.subject_id:
                    row[msg_response.msg_id + "_Response"] = msg_response.answer
                    row[msg_response.msg_id + "_Response_Time"] = msg_response.resp_time
        for row in rows:
            for l_response in likert_responses:
                if l_response.associated_msg != "System" and row['SubjectID'] == l_response.subject_id:
                    row[l_response.associated_msg + "_"+l_response.likert_code +"_Response" ] = l_response.answer
                elif l_response.associated_msg == "System" and row['SubjectID'] == l_response.subject_id:
                    row[l_response.trial_id + "_" + l_response.likert_code + "_Response"] = l_response.answer

        # reindex forces the correct ordering of the columns
        new_df = pandas.DataFrame(data= rows, columns=new_columns).reindex(columns=new_columns)
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
                        df['msgblk_v3'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LM2', df['key3'][ind],
                       df['msgblk_v3'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LM3', df['key4'][ind],
                       df['msgblk_v3'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LM1', df['key6'][ind],
                      df['msgblk_v6'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LM2', df['key7'][ind],
                       df['msgblk_v6'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LM3', df['key8'][ind],
                       df['msgblk_v6'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LM1', df['key10'][ind],
                       df['msgblk_v9'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LM2', df['key11'][ind],
                       df['msgblk_v9'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LM3', df['key12'][ind],
                       df['msgblk_v9'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LB1', df['key13'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LB2', df['key14'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LB3', df['key15'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LB4', df['key16'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LB5', df['key17'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LB6', df['key18'][ind]))
    likert_response.append(
        LikertResponse(df['SubjectID'][ind], df['trlid'][ind], 'LB7', df['key19'][ind]))


def write_csv(cleaned_data):
    for named_frame in cleaned_data:
        # create a new csv in the clean_data_path directory, with the same name
        # as the original file
        path = clean_data_path / named_frame.name
        frame = named_frame.df
        frame_index = frame.index.name

        frame.to_csv(path, index=frame_index, header=True)