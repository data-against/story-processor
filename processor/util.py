def chunks(lst, n):
    """
    Yield successive n-sized chunks from lst.
    https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
