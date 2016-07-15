def make_mv_answer(crowd):
    mv_answer = {}
    # For each task
    for (i, r) in zip(crowd.table["id"], crowd.table["result"]):
        if str(i) not in mv_answer:
            mv_answer[str(i)] = ""
        if len(r) == 0 :
            mv_answer[str(i)] = ""
        else:
            count = {}
            # For each assignment
            for j in r:
                if j["info"] in count:
                    count[j["info"]] += 1
                else:
                    count[j["info"]] = 1
            # Find the mode
            max_count = -1
            current_answer = ""
            for key, value in count.items():
                if value > max_count:
                    max_count = value
                    current_answer = key
            mv_answer[str(i)] = current_answer

    return mv_answer
