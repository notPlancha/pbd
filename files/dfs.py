from files import csvFile, shemas as s

test               = csvFile(r".\data\test.csv", s.test)
sample_submissions = csvFile(r".\data\sample_submission.csv", s.sample_sub)
train_labels       = csvFile(r".\data\train_labels.csv", s.train_labs)
train              = csvFile(r".\data\train.csv", s.train)

CsvFiles = [test, sample_submissions, train_labels, train]

def readAllCsvs(spark, force = False, **kwargs):
    for i in CsvFiles:
        try:
            i.read(spark, force = force, **kwargs)
        except AssertionError:
            print(i.path, "already read, skipping (use force = True to read it anyways")
            continue
    return [i.df for i in CsvFiles]