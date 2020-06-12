class LikertResponse(object):
    def __init__(self, subject_id, trial_id, likert_code, answer, associated_msg="System"):
        self.subject_id = subject_id
        self.trial_id = trial_id
        self.likert_code = likert_code
        self.answer = answer
        self.associated_msg = associated_msg

    def __str__(self):
        return str(self.likert_code) + "_" + str(self.trial_id) + "_" + str(self.subject_id)