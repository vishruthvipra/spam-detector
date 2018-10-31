from elasticsearch import Elasticsearch
import string
import pickle
import operator
import operator


LABEL_PATH = '/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN7/trec07p/full/index'
RESULT_PATH = '/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN7/result/'
INDEX = 'spam'
DOC_TYPE = 'document'

docGrade = {}
dictionary = {}
index = 0
train_set = []
test_set = []
train_index = []
test_index = []


def main():
    es = Elasticsearch()
    spamHam()
    train_test()
    write_feature_matrix(es)

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

def train_test():
    train_path = RESULT_PATH + 'train_set'
    test_path = RESULT_PATH + 'test_set'
    train_fh = open(train_path, "r")
    for line in train_fh.readlines():
        doc_id = string.strip(line, '\n')
        train_set.append(doc_id)
    train_fh.close()
    test_fh = open(test_path, "r")
    for line in test_fh.readlines():
        doc_id = string.strip(line, '\n')
        test_set.append(doc_id)
    test_fh.close()

def write_feature_matrix(es):
    train_matrix_path = RESULT_PATH + 'train_feature_matrix1'
    test_matrix_path = RESULT_PATH + 'test_feature_matrix1'
    train_fh = open(train_matrix_path, "w")
    test_fh = open(test_matrix_path, "w")
    count = 0

    for doc_id in sorted(docGrade, key=operator.itemgetter(1)):
        feature_map = query_doc(es, doc_id)
        label = docGrade[doc_id]
        if doc_id in train_set:
            writingFile(label, feature_map, train_fh)
            train_index.append(doc_id)
        elif doc_id in test_set:
            writingFile(label, feature_map, test_fh)
            test_index.append(doc_id)
        count += 1
    train_fh.close()
    test_fh.close()
    indextozfile('train_index', train_index)
    indextozfile('test_index', test_index)


def query_doc(es,doc_id):
    global index
    resp = es.termvectors(index=INDEX,doc_type=DOC_TYPE, id=doc_id)
    wordTF = {}
    try:
        terms = resp['term_vectors']['text']['terms']
    except:
        return wordTF

    for word in terms:
        tf = terms[word]['term_freq']
        if word not in dictionary:
            index += 1
            dictionary[word] = index
            wordTF[index] = tf
        else:
            word_index = dictionary[word]
            wordTF[word_index] = tf
    print "The length of", doc_id, "is:", len(wordTF)
    return wordTF




def writingFile(label, feature_matrix, fh):
    fh.write('%d'%label)
    if feature_matrix:
        for index in sorted(feature_matrix):
            fh.write(' %d:%d'%(index, feature_matrix[index]))
    fh.write('\n')

def indextozfile(filename, index):
    outpath = RESULT_PATH + '/' + filename
    fh = open(outpath, "w")
    for doc_id in index:
        fh.write('%s\n'%doc_id)
    fh.close()




if __name__ == '__main__':
    main()