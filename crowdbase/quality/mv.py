def make_mv_answer(task_result):
    mv_answer = [None] * len(task_result)
    # For each task
    for i, r in enumerate(task_result):
        if len(r) == 0 :
            mv_answer[i] = ""
        else:
            count = {}
            # For each assignment
            for j in r:
                if j["result_info"] in count:
                    count[j["result_info"]] += 1
                else:
                    count[j["result_info"]] = 1
            # Find the mode
            max_count = -1
            current_answer = ""
            for key, value in count.items():
                if value > max_count:
                    max_count = value
                    current_answer = key
            mv_answer[i] = current_answer
    return mv_answer
