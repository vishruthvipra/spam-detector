from elasticsearch import Elasticsearch
from sklearn import tree
import string
import operator

LABEL_PATH = '/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN7/trec07p/full/index'
RESULT_PATH = '/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN7/result'
SPAM_PATH = '/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN7/spam_words.txt'
# spamWords = ['free', 'win', 'porn','click here']
spamWords = []
train_labels = []
test_labels = []
train_ft = []
test_ft = []
train_set = set()
test_set = set()
train_index = []
test_index = []
docGrade = {}
featuretfdict = {}
train_result = {}
test_result = {}

INDEX = 'spam'
DOC_TYPE = 'document'


def main():
    spamcheck()
    spamHam()
    es = Elasticsearch()
    featureTF(es)
    featMatrix()
    decision_tree_model()


def spamcheck():
    global spamWords

    f = open(SPAM_PATH, "r")
    for line in f.readlines():
        line = string.strip(line, '\n')
        spamWords.append(line)

    f.close()


def spamHam():
    fh = open(LABEL_PATH, "r")
    for line in fh.readlines():
        new_line = string.strip(line, '\n')
        [grade, path] = new_line.split(' ')
        doc_id = path.split('/')[-1]
        if grade == 'spam':
            grade = 1
        else:
            grade = 0
        docGrade[doc_id] = grade


def featureTF(es):
    for gram in spamWords:
        query = {
            "query": {
                "match": {
                    "text": gram
                }
            }
        }

        resp = es.search(index=INDEX, doc_type=DOC_TYPE, body=query, explain=False, scroll="100m", size=100)
        scrollId = resp['_scroll_id']
        total_number = resp['hits']['total']
        print "The total for", gram, "is:", total_number
        while True:
            if resp is None:
                print "resp none"
                break
            for i in resp['hits']['hits']:
                tf = i['_score']
                doc_id = i['_id']
                class_set = i['_source']['class']

                if class_set == 'train':
                    train_set.add(doc_id)
                else:
                    test_set.add(doc_id)
                if doc_id not in featuretfdict:
                    featuretfdict[doc_id] = {gram: tf}
                else:
                    featuretfdict[doc_id][gram] = tf

            resp = es.scroll(scroll_id=scrollId, scroll='1000ms')
            if len(resp['hits']['hits']) > 0:
                scrollId = resp['_scroll_id']
            else:
                break


def featMatrix():
    for doc_id in featuretfdict:
        map = featuretfdict[doc_id]
        vector = []
        label = docGrade[doc_id]
        for word in spamWords:
            if word in map:
                vector.append(map[word])
            else:
                vector.append(0)
        if doc_id in train_set:
            train_index.append(doc_id)
            train_ft.append(vector)
            train_labels.append(label)
        elif doc_id in test_set:
            test_index.append(doc_id)
            test_ft.append(vector)
            test_labels.append(label)


def decision_tree_model():
    clf = tree.DecisionTreeClassifier()
    clf.fit(train_ft, train_labels)
    train_result = clf.predict(train_ft)
    test_result = clf.predict(test_ft)
    train_result_map, test_result_map = interpretRes(train_result, test_result)
    write_to_file(train_result_map, "decision_tree_train_performance_trial_b")
    write_to_file(test_result_map, "decision_tree_test_performance_trial_b")


def interpretRes(raw_train_result, raw_test_result):
    train_result = {}
    test_result = {}
    train_accuracy = 0
    test_accuracy = 0
    for i in range(len(raw_train_result)):
        doc_id = train_index[i]
        score = raw_train_result[i]
        train_result[doc_id] = score
        if int(score) == int(docGrade[doc_id]):
            train_accuracy += 1
    train_acc = float(train_accuracy) / len(train_index)
    for i in range(len(raw_test_result)):
        doc_id = test_index[i]
        score = raw_test_result[i]
        test_result[doc_id] = score
        if int(score) == int(docGrade[doc_id]):
            test_accuracy += 1
    test_acc = float(test_accuracy) / len(test_index)
    print "\n\nThe train set accuracy is:", train_acc * 100
    print "The test set accuracy is:", test_acc * 100

    return train_result, test_result


def write_to_file(result_map, filename):
    outpath = RESULT_PATH + '/' + filename
    fh = open(outpath, "w")
    sorted_result = sorted(result_map.items(), key=operator.itemgetter(1), reverse=True)
    count = 1
    for pair in sorted_result:
        fh.write('%s %d %f Exp\n' % (pair[0], count, pair[1]))
        count += 1
    fh.close()


if __name__ == '__main__':
    main()
