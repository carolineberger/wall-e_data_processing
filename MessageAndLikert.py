class MessageAndLikert(object):
    def __init__(self, subject_id, trial_id, message_response, likert_responses):
        self.subject_id = subject_id
        self.trial_id = trial_id
        self.message_response = message_response
        self.likert_responses = likert_responses