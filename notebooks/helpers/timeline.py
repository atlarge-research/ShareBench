# line sweep algorithm to get a timeline from a list of load changes

def get_timeline(changes, end_time=None):
    total = 0
    timeline = []

    if len(changes) > 0:
        first_time, first_delta = changes[0]
        last_time = first_time - 1
        for time, delta in changes:
            if (time != last_time):
                timeline.append((last_time, total))
                timeline.append((time, total))
                last_time = time
            total += delta
        timeline.append((last_time, total))

        if end_time != None:
            if (end_time > last_time):
                timeline.append((end_time - 1, total))
                timeline.append((end_time, 0))
                
    return timeline