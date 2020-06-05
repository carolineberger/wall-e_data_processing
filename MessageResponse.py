class MessageResponse(object):
    def __init__(self, subject_id, trial_id, msg_id, answer, resp_time):
        self.subject_id = subject_id
        self.trial_id = trial_id
        self.msg_id = msg_id
        self.answer = answer
        self.resp_time = resp_time

    def __str__(self):
        return str(self.subject_id) + "_" + str(self.msg_id)