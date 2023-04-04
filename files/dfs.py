from files import csvFile
test               = csvFile(r".\data\test.csv")
sample_submissions = csvFile(r".\data\sample_submission.csv")
train_labels       = csvFile(r".\data\train_labels.csv")
train              = csvFile(r".\data\train.csv")

CsvFiles = [test, sample_submissions, train_labels, train]

def readAllCsvs(spark, force = False, **kwargs):
    for i in CsvFiles:
        try:
            i.read(spark, force = force, **kwargs)
        except AssertionError:
            print(i.path, "already read, skipping (use force = True to read it anyways")
            continue
    return [i.df for i in CsvFiles]