from elasticsearch import Elasticsearch
from elasticsearch import helpers
import os
import re
import random
import string

PATH = '/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN7/trec07p/data'
LABEL_PATH = '/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN7/trec07p/full/index'
TRAIN = 'train'
TEST = 'test'
INDEX = 'spam'
DOC_TYPE = 'document'
RESULT_PATH = '/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN7/result/'

docGrade = {}
spamlist = []
hamlist = []

def main():
    spamHam()
    es = Elasticsearch()
    startIndexer(es)



def spamHam():
    global docGrade
    global spamlist
    global hamlist

    fh = open(LABEL_PATH, "r")
    for line in fh.readlines():
        new_line = string.strip(line, '\n')
        [grade, url] = new_line.split(' ')
        doc_id = url.split('/')[-1]
        if grade == 'spam':
            spamlist.append(doc_id)
            grade = 1
        else:
            hamlist.append(doc_id)
            grade = 0
        docGrade[doc_id] = grade


def startIndexer(es):
    global docGrade
    global spamlist
    global hamlist

    spamlimit = len(spamlist) * 80 / 100
    hamlimit = len(hamlist) * 80 / 100


    files = os.listdir(PATH)
    training = RESULT_PATH + 'train_set'
    testing = RESULT_PATH + 'test_set'
    train_f = open(training, "w")
    test_f = open(testing, "w")
    count = 1
    spam_count = 0
    ham_count = 0
    actions = []

    for x in files:
        if 'inmail' in x:
            text = cleanBody(x)
            label = docGrade[x]

            if label < 1:
                ham_count += 1
                if ham_count < hamlimit:
                    class_set = TRAIN
                else:
                    class_set = TEST
            else:
                spam_count += 1
                if spam_count < spamlimit:
                    class_set = TRAIN
                else:
                    class_set = TEST

            actions += [
                {
                    "_index": INDEX,
                    "_type": DOC_TYPE,
                    "_id": x,
                    "_source": {
                        "docno": x,
                        "class": class_set,
                        "text": text,
                        "label": label
                    }
                }
            ]

            print x
            if class_set == TRAIN:
                train_f.write('%s\n' % x)
            else:
                test_f.write('%s\n' % x)
            count += 1
    train_f.close()
    test_f.close()

    print count




def cleanBody(filename):
    filepath = PATH + '/' + filename
    fh = open(filepath, "r")
    body = []
    content_begin = False
    for line in fh.readlines():
        try:
            line = line.replace('\n', '').replace('_', '').replace('\t', '')
            line = line.encode('utf-8')
            line = line.lower()
        except:
            line = ''
        if line.startswith('subject'):
            body.append(line)
        elif line.startswith('line') or len(line) == 0 or line == '\n':
            content_begin = True
        elif content_begin:
            if ' ' not in line and len(line) > 20:
                continue
            if line.startswith('----'):
                continue
            if line.startswith('content-type'):
                continue
            if line.startswith('content-transfer-encoding'):
                continue
            body.append(line)

    regression = re.compile(r'<(.*?)>')
    text = ' '.join(body)
    res = regression.subn('', text)
    new_content = res[0]
    new_content = new_content.replace('=', '')
    fh.close()
    return new_content




if __name__ == '__main__':
    main()