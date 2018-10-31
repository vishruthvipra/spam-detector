TRIALA = '/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN7/result/decision_tree_train_performance_trial_a'
TRIALB = '/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN7/result/decision_tree_train_performance_trial_b'
OUTPUTTOTAL = '/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN7/output_total'


def main():
    f = open(TRIALA, "r")
    score = 0
    for line in f.readlines()[:50]:
        _, _, grade, _ = line.split()
        grade = float(grade)
        score += grade

    accuracy = score / 50 * 100
    print "Accuracy for top 50 manual:", accuracy

    f.close()

    f = open(TRIALB, "r")
    score = 0
    for line in f.readlines()[:50]:
        _, _, grade, _ = line.split()
        grade = float(grade)
        score += grade

    accuracy = score / 50 * 100
    print "Accuracy for top 50 spamwords:", accuracy

    f.close()

    f = open(OUTPUTTOTAL, "r")

    allscores = []
    score = 0
    for line in f.readlines()[1:]:
        _, grade, _ = line.split()
        grade = float(grade)
        allscores.append(grade)

    newscores = sorted(allscores, reverse=True)

    for grade in newscores[:50]:
        score += grade

    accuracy = score / 50 * 100
    print "Accuracy for top 50 unigrams:", accuracy

    f.close()

if __name__ == '__main__':
    main()